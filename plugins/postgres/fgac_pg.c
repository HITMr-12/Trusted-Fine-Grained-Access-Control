#include "postgres.h"
#include "fmgr.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "commands/explain.h"
#include "executor/nodeCustom.h"
#include "executor/executor.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planner.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

PG_MODULE_MAGIC;

static set_rel_pathlist_hook_type previous_set_rel_pathlist_hook = NULL;
static planner_hook_type previous_planner_hook = NULL;
static ProcessUtility_hook_type previous_process_utility_hook = NULL;
static char *catalog_host = NULL, *remote_host = NULL, *user_token = NULL;
static int catalog_port = 8001, remote_port = 8002;

typedef struct FgacRow { int id; char *region; int amount; char *owner; char *card; } FgacRow;
typedef struct FgacScanState {
    CustomScanState css;
    FgacRow *rows;
    int row_count;
    int cursor;
} FgacScanState;

/*
 * Pushdown is deliberately conservative.  Every base-relation qual remains
 * in scan.plan.qual and is rechecked by PostgreSQL.  The JSON generated here
 * is therefore an optimization only; it must never become the sole owner of
 * query semantics.
 */
static void append_json_string(StringInfo output, const char *value)
{
    const unsigned char *cursor = (const unsigned char *) value;
    appendStringInfoChar(output, '"');
    while (*cursor) {
        switch (*cursor) {
            case '"': appendStringInfoString(output, "\\\""); break;
            case '\\': appendStringInfoString(output, "\\\\"); break;
            case '\b': appendStringInfoString(output, "\\b"); break;
            case '\f': appendStringInfoString(output, "\\f"); break;
            case '\n': appendStringInfoString(output, "\\n"); break;
            case '\r': appendStringInfoString(output, "\\r"); break;
            case '\t': appendStringInfoString(output, "\\t"); break;
            default:
                if (*cursor < 0x20) appendStringInfo(output, "\\u%04x", *cursor);
                else appendStringInfoChar(output, *cursor);
        }
        cursor++;
    }
    appendStringInfoChar(output, '"');
}

static bool remote_scalar_type(Oid type_oid)
{
    return type_oid == INT2OID || type_oid == INT4OID || type_oid == INT8OID ||
           type_oid == TEXTOID || type_oid == VARCHAROID || type_oid == BOOLOID;
}

static bool serialize_remote_expr(Node *expression, Oid relation_oid,
                                  Index scan_relid, StringInfo output);

static bool serialize_remote_const(Const *constant, StringInfo output)
{
    Oid output_function; bool variable_length;
    char *value;

    if (constant->constisnull) {
        appendStringInfoString(output,
            "{\"op\":\"literal\",\"data_type\":\"null\",\"value\":null}");
        return true;
    }
    if (!remote_scalar_type(constant->consttype)) return false;

    appendStringInfoString(output, "{\"op\":\"literal\",\"data_type\":");
    if (constant->consttype == BOOLOID) {
        appendStringInfoString(output, "\"boolean\",\"value\":");
        appendStringInfoString(output, DatumGetBool(constant->constvalue) ? "true}" : "false}");
        return true;
    }
    if (constant->consttype == INT2OID || constant->consttype == INT4OID ||
        constant->consttype == INT8OID) {
        getTypeOutputInfo(constant->consttype, &output_function, &variable_length);
        value = OidOutputFunctionCall(output_function, constant->constvalue);
        appendStringInfoString(output, "\"long\",\"value\":");
        appendStringInfoString(output, value); appendStringInfoChar(output, '}');
        return true;
    }
    getTypeOutputInfo(constant->consttype, &output_function, &variable_length);
    value = OidOutputFunctionCall(output_function, constant->constvalue);
    appendStringInfoString(output, "\"string\",\"value\":");
    append_json_string(output, value); appendStringInfoChar(output, '}');
    return true;
}

static bool serialize_remote_bool_args(List *arguments, const char *operator,
                                       Oid relation_oid, Index scan_relid,
                                       StringInfo output)
{
    ListCell *cell;
    StringInfoData accumulated;
    bool first = true;

    initStringInfo(&accumulated);
    foreach(cell, arguments) {
        StringInfoData child;
        initStringInfo(&child);
        if (!serialize_remote_expr((Node *) lfirst(cell), relation_oid, scan_relid, &child))
            return false;
        if (first) {
            appendStringInfoString(&accumulated, child.data); first = false;
        } else {
            StringInfoData combined;
            initStringInfo(&combined);
            appendStringInfo(&combined,
                "{\"op\":\"%s\",\"left\":%s,\"right\":%s}",
                operator, accumulated.data, child.data);
            accumulated = combined;
        }
    }
    if (first) return false;
    appendStringInfoString(output, accumulated.data);
    return true;
}

static bool serialize_remote_expr(Node *expression, Oid relation_oid,
                                  Index scan_relid, StringInfo output)
{
    if (expression == NULL) return false;
    if (IsA(expression, Var)) {
        Var *variable = (Var *) expression;
        char *attribute;
        if (variable->varno != scan_relid || variable->varlevelsup != 0 ||
            variable->varattno <= 0 || !remote_scalar_type(variable->vartype)) return false;
        attribute = get_attname(relation_oid, variable->varattno, false);
        appendStringInfoString(output, "{\"op\":\"column\",\"name\":");
        append_json_string(output, attribute); appendStringInfoChar(output, '}');
        return true;
    }
    if (IsA(expression, Const)) return serialize_remote_const((Const *) expression, output);
    if (IsA(expression, OpExpr)) {
        OpExpr *operation = (OpExpr *) expression;
        Node *left, *right; Oid left_type, right_type; char *name; const char *remote_op;
        StringInfoData left_json, right_json;
        if (list_length(operation->args) != 2) return false;
        left = linitial(operation->args); right = lsecond(operation->args);
        left_type = exprType(left); right_type = exprType(right);
        if (!remote_scalar_type(left_type) || !remote_scalar_type(right_type)) return false;
        /* Avoid cross-type coercion and locale-sensitive string ordering. */
        if (left_type != right_type && !((left_type == INT2OID || left_type == INT4OID || left_type == INT8OID) &&
                                         (right_type == INT2OID || right_type == INT4OID || right_type == INT8OID)))
            return false;
        name = get_opname(operation->opno);
        if (!name) return false;
        if (strcmp(name, "=") == 0) remote_op = "eq";
        else if (strcmp(name, "<>") == 0) remote_op = "neq";
        else if (strcmp(name, ">") == 0 && left_type != TEXTOID && left_type != VARCHAROID) remote_op = "gt";
        else if (strcmp(name, ">=") == 0 && left_type != TEXTOID && left_type != VARCHAROID) remote_op = "gte";
        else if (strcmp(name, "<") == 0 && left_type != TEXTOID && left_type != VARCHAROID) remote_op = "lt";
        else if (strcmp(name, "<=") == 0 && left_type != TEXTOID && left_type != VARCHAROID) remote_op = "lte";
        else return false;
        initStringInfo(&left_json); initStringInfo(&right_json);
        if (!serialize_remote_expr(left, relation_oid, scan_relid, &left_json) ||
            !serialize_remote_expr(right, relation_oid, scan_relid, &right_json)) return false;
        appendStringInfo(output,
            "{\"op\":\"%s\",\"left\":%s,\"right\":%s}",
            remote_op, left_json.data, right_json.data);
        return true;
    }
    if (IsA(expression, BoolExpr)) {
        BoolExpr *boolean = (BoolExpr *) expression;
        if (boolean->boolop == AND_EXPR)
            return serialize_remote_bool_args(boolean->args, "and", relation_oid, scan_relid, output);
        if (boolean->boolop == OR_EXPR)
            return serialize_remote_bool_args(boolean->args, "or", relation_oid, scan_relid, output);
        if (boolean->boolop == NOT_EXPR && list_length(boolean->args) == 1) {
            StringInfoData child; initStringInfo(&child);
            if (!serialize_remote_expr(linitial(boolean->args), relation_oid, scan_relid, &child)) return false;
            appendStringInfo(output, "{\"op\":\"not\",\"input\":%s}", child.data); return true;
        }
        return false;
    }
    if (IsA(expression, NullTest)) {
        NullTest *test = (NullTest *) expression; StringInfoData child;
        initStringInfo(&child);
        if (!serialize_remote_expr((Node *) test->arg, relation_oid, scan_relid, &child)) return false;
        appendStringInfo(output, "{\"op\":\"%s\",\"input\":%s}",
            test->nulltesttype == IS_NULL ? "is_null" : "is_not_null", child.data);
        return true;
    }
    return false;
}

static char *serialize_pushable_quals(List *quals, Oid relation_oid, Index scan_relid)
{
    ListCell *cell; List *pushable = NIL;
    foreach(cell, quals) {
        StringInfoData expression; initStringInfo(&expression);
        if (serialize_remote_expr((Node *) lfirst(cell), relation_oid, scan_relid, &expression))
            pushable = lappend(pushable, makeString(expression.data));
    }
    if (pushable == NIL) return pstrdup("");
    StringInfoData result; initStringInfo(&result);
    ListCell *json_cell; bool first = true;
    foreach(json_cell, pushable) {
        char *json = strVal(lfirst(json_cell));
        if (first) { appendStringInfoString(&result, json); first = false; }
        else {
            StringInfoData combined; initStringInfo(&combined);
            appendStringInfo(&combined, "{\"op\":\"and\",\"left\":%s,\"right\":%s}", result.data, json);
            result = combined;
        }
    }
    return result.data;
}

static char *http_call(const char *host, int port, const char *method,
                       const char *path, const char *token, const char *body,
                       int *status)
{
    struct addrinfo hints = {0}, *addresses = NULL, *address;
    int socket_fd = -1, rc;
    char port_text[16];
    StringInfoData request, response;
    char chunk[4096];
    ssize_t count;

    snprintf(port_text, sizeof(port_text), "%d", port);
    hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
    rc = getaddrinfo(host, port_text, &hints, &addresses);
    if (rc != 0) ereport(ERROR, (errmsg("FGAC DNS failed for %s: %s", host, gai_strerror(rc))));
    for (address = addresses; address; address = address->ai_next) {
        socket_fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        if (socket_fd >= 0 && connect(socket_fd, address->ai_addr, address->ai_addrlen) == 0) break;
        if (socket_fd >= 0) close(socket_fd); socket_fd = -1;
    }
    freeaddrinfo(addresses);
    if (socket_fd < 0) ereport(ERROR, (errmsg("FGAC cannot connect to %s:%d", host, port)));

    initStringInfo(&request); initStringInfo(&response);
    appendStringInfo(&request, "%s %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n", method, path, host);
    if (token && token[0]) appendStringInfo(&request, "Authorization: Bearer %s\r\n", token);
    if (body) appendStringInfo(&request, "Content-Type: application/json\r\nContent-Length: %zu\r\n", strlen(body));
    appendStringInfoString(&request, "\r\n"); if (body) appendStringInfoString(&request, body);
    if (write(socket_fd, request.data, request.len) != request.len) ereport(ERROR, (errmsg("FGAC HTTP write failed")));
    while ((count = read(socket_fd, chunk, sizeof(chunk))) > 0) appendBinaryStringInfo(&response, chunk, count);
    close(socket_fd);
    if (sscanf(response.data, "HTTP/%*s %d", status) != 1) ereport(ERROR, (errmsg("Invalid FGAC HTTP response")));
    char *payload = strstr(response.data, "\r\n\r\n");
    if (!payload) ereport(ERROR, (errmsg("Invalid FGAC HTTP headers")));
    return pstrdup(payload + 4);
}

static bool discover_governed(Oid relation_oid, char **relation_id)
{
    char *name = get_rel_name(relation_oid), path[512]; int status;
    char *body;
    if (!name) return false;
    snprintf(path, sizeof(path), "/v2/relations/public/%s", name);
    body = http_call(catalog_host, catalog_port, "GET", path, NULL, NULL, &status);
    if (status == 404) return false;
    if (status / 100 != 2) ereport(ERROR, (errmsg("Catalog discovery rejected relation: %s", body)));
    if (!strstr(body, "\"governed\":true")) return false;
    char *start = strstr(body, "\"relation_id\":\"");
    if (!start) ereport(ERROR, (errmsg("Catalog response misses relation_id")));
    start += strlen("\"relation_id\":\""); char *end = strchr(start, '\"');
    *relation_id = pnstrdup(start, end - start); return true;
}

/*
 * A governed relation is read-only from an ordinary engine.  Reject the
 * complete statement before standard_planner can construct a ModifyTable
 * plan (or reach a CustomScan executor that was only designed for reads).
 *
 * We deliberately reject a non-SELECT statement when a governed relation is
 * either its target or one of its sources.  This prevents both direct writes
 * and egress patterns such as INSERT INTO local_table SELECT ... FROM orders.
 */
static bool query_references_governed_relation(Query *query)
{
    ListCell *cell;

    foreach(cell, query->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(cell);
        char *relation_id = NULL;

        if (rte->rtekind == RTE_RELATION &&
            discover_governed(rte->relid, &relation_id))
            return true;
        if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL &&
            query_references_governed_relation(rte->subquery))
            return true;
    }

    foreach(cell, query->cteList) {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(cell);
        if (IsA(cte->ctequery, Query) &&
            query_references_governed_relation((Query *) cte->ctequery))
            return true;
    }
    return false;
}

static PlannedStmt *fgac_planner(Query *parse, const char *query_string,
                                 int cursor_options, ParamListInfo bound_params)
{
    if ((parse->commandType != CMD_SELECT || parse->hasModifyingCTE) &&
        query_references_governed_relation(parse))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("FGAC governed relations are read-only in the local engine"),
                 errdetail("DML involving governed data must be executed by an authorized Remote write path.")));

    if (previous_planner_hook)
        return previous_planner_hook(parse, query_string, cursor_options, bound_params);
    return standard_planner(parse, query_string, cursor_options, bound_params);
}

static void reject_governed_utility_relation(RangeVar *range_variable)
{
    Oid relation_oid;
    char *relation_id = NULL;

    if (range_variable == NULL)
        return;
    relation_oid = RangeVarGetRelid(range_variable, NoLock, true);
    if (OidIsValid(relation_oid) &&
        discover_governed(relation_oid, &relation_id))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("FGAC governed relations are read-only in the local engine"),
                 errdetail("COPY and TRUNCATE cannot bypass the authorized Remote path.")));
}

static void fgac_process_utility(PlannedStmt *pstmt, const char *query_string,
                                 bool read_only_tree,
                                 ProcessUtilityContext context,
                                 ParamListInfo params,
                                 QueryEnvironment *query_environment,
                                 DestReceiver *destination,
                                 QueryCompletion *completion)
{
    Node *utility = pstmt->utilityStmt;

    if (IsA(utility, CopyStmt)) {
        CopyStmt *copy_statement = (CopyStmt *) utility;
        reject_governed_utility_relation(copy_statement->relation);
    } else if (IsA(utility, TruncateStmt)) {
        TruncateStmt *truncate_statement = (TruncateStmt *) utility;
        ListCell *cell;
        foreach(cell, truncate_statement->relations)
            reject_governed_utility_relation((RangeVar *) lfirst(cell));
    }

    if (previous_process_utility_hook)
        previous_process_utility_hook(pstmt, query_string, read_only_tree,
                                      context, params, query_environment,
                                      destination, completion);
    else
        standard_ProcessUtility(pstmt, query_string, read_only_tree,
                                context, params, query_environment,
                                destination, completion);
}

static Plan *plan_custom_path(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
                              List *tlist, List *clauses, List *custom_plans);
static Node *create_custom_scan_state(CustomScan *scan);
static void begin_custom_scan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *exec_custom_scan(CustomScanState *node);
static void end_custom_scan(CustomScanState *node);
static void rescan_custom_scan(CustomScanState *node);
static void explain_custom_scan(CustomScanState *node, List *ancestors, ExplainState *es);

static CustomPathMethods path_methods = {"FgacRemotePath", plan_custom_path};
static CustomScanMethods scan_methods = {"FgacRemoteScan", create_custom_scan_state};
static CustomExecMethods exec_methods = {
    .CustomName = "FgacRemoteScan",
    .BeginCustomScan = begin_custom_scan,
    .ExecCustomScan = exec_custom_scan,
    .EndCustomScan = end_custom_scan,
    .ReScanCustomScan = rescan_custom_scan,
    .ExplainCustomScan = explain_custom_scan
};

static void fgac_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
    char *relation_id = NULL;
    if (previous_set_rel_pathlist_hook) previous_set_rel_pathlist_hook(root, rel, rti, rte);
    if (rte->rtekind != RTE_RELATION || !discover_governed(rte->relid, &relation_id)) return;
    CustomPath *path = makeNode(CustomPath);
    path->path.pathtype = T_CustomScan; path->path.parent = rel; path->path.pathtarget = rel->reltarget;
    path->path.param_info = NULL; path->path.parallel_aware = false; path->path.parallel_safe = false;
    path->path.rows = rel->rows; path->path.startup_cost = 10; path->path.total_cost = 100 + rel->rows;
    path->flags = 0; path->custom_paths = NIL; path->custom_private = list_make1(makeString(relation_id));
    path->methods = &path_methods;
    rel->pathlist = NIL; rel->partial_pathlist = NIL; add_path(rel, &path->path);
}

static Plan *plan_custom_path(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
                              List *tlist, List *clauses, List *custom_plans)
{
    CustomScan *scan = makeNode(CustomScan);
    RangeTblEntry *rte = (RangeTblEntry *) list_nth(root->parse->rtable, rel->relid - 1);
    Relation relation = table_open(rte->relid, NoLock);
    TupleDesc descriptor = RelationGetDescr(relation);
    List *scan_tlist = NIL;
    List *actual_quals;
    char *remote_filter;
    int attno;

    for (attno = 1; attno <= descriptor->natts; attno++) {
        Form_pg_attribute attribute = TupleDescAttr(descriptor, attno - 1);
        Var *var = makeVar(rel->relid, attno,
                           attribute->atttypid,
                           attribute->atttypmod,
                           attribute->attcollation, 0);
        scan_tlist = lappend(scan_tlist,
                            makeTargetEntry((Expr *) var, attno,
                                            pstrdup(NameStr(attribute->attname)),
                                            false));
    }
    table_close(relation, NoLock);
    /*
     * Preserve the semantic target list built by PostgreSQL.  It carries
     * engine-private identity used by upper Aggregate/Sort/Window nodes.
     * The full row returned by Remote is a separate physical scan tuple.
     */
    scan->scan.plan.targetlist = copyObject(tlist);
    actual_quals = extract_actual_clauses(clauses, false);
    scan->scan.plan.qual = actual_quals;
    scan->scan.scanrelid = rel->relid;
    remote_filter = serialize_pushable_quals(actual_quals, rte->relid, rel->relid);
    scan->custom_plans = custom_plans;
    scan->custom_private = lappend(list_copy(path->custom_private), makeString(remote_filter));
    scan->custom_scan_tlist = scan_tlist;
    scan->flags = CUSTOMPATH_SUPPORT_PROJECTION;
    scan->methods = &scan_methods;
    return &scan->scan.plan;
}

static Node *create_custom_scan_state(CustomScan *scan)
{
    FgacScanState *state = palloc0(sizeof(FgacScanState));
    NodeSetTag(state, T_CustomScanState); state->css.methods = &exec_methods; return (Node *) state;
}

static void begin_custom_scan(CustomScanState *node, EState *estate, int eflags)
{
    FgacScanState *state = (FgacScanState *) node; CustomScan *scan = (CustomScan *) node->ss.ps.plan;
    char *relation_id = strVal(linitial(scan->custom_private));
    char *remote_filter = strVal(lsecond(scan->custom_private)); int status;
    StringInfoData body; initStringInfo(&body);
    appendStringInfoString(&body,
      "{\"version\":2,\"schema_version\":\"1\",\"root\":{\"op\":\"project\",\"columns\":[\"id\",\"region\",\"amount\",\"owner\",\"card_no\"],\"input\":");
    if (remote_filter[0])
        appendStringInfo(&body, "{\"op\":\"filter\",\"condition\":%s,\"input\":", remote_filter);
    appendStringInfo(&body, "{\"op\":\"governed_scan\",\"relation\":\"%s\"}", relation_id);
    if (remote_filter[0]) appendStringInfoChar(&body, '}');
    appendStringInfoString(&body, "}}");
    char *response = http_call(remote_host, remote_port, "POST", "/v2/subplans", user_token, body.data, &status);
    if (status / 100 != 2) ereport(ERROR, (errmsg("Remote FGAC rejected plan: %s", response)));
    char *cursor = strstr(response, "\"inline_rows\":[");
    if (!cursor) ereport(ERROR, (errmsg("Remote response misses inline_rows")));
    cursor += strlen("\"inline_rows\":["); state->rows = palloc0(sizeof(FgacRow) * 1024);
    while (*cursor && *cursor != ']') {
        while (*cursor && *cursor != '[' && *cursor != ']') cursor++;
        if (*cursor != '[') break;
        FgacRow *row = &state->rows[state->row_count]; char region[256], owner[256], card[256]; int used = 0;
        if (sscanf(cursor, "[%d,\"%255[^\"]\",%d,\"%255[^\"]\",\"%255[^\"]\"]%n",
                   &row->id, region, &row->amount, owner, card, &used) != 5)
            ereport(ERROR, (errmsg("Unsupported Remote row encoding near %.80s", cursor)));
        row->region = pstrdup(region); row->owner = pstrdup(owner); row->card = pstrdup(card);
        state->row_count++; cursor += used;
    }
}

static TupleTableSlot *fetch_remote_tuple(ScanState *scan_state)
{
    CustomScanState *node = (CustomScanState *) scan_state;
    FgacScanState *state = (FgacScanState *) node; TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    TupleDesc descriptor = slot->tts_tupleDescriptor;
    int column;
    ExecClearTuple(slot); if (state->cursor >= state->row_count) return slot;
    FgacRow *row = &state->rows[state->cursor++];
    memset(slot->tts_isnull, false, sizeof(bool) * descriptor->natts);
    for (column = 0; column < descriptor->natts; column++) {
        int source_attno = column + 1;
        switch (source_attno) {
            case 1: slot->tts_values[column] = Int32GetDatum(row->id); break;
            case 2: slot->tts_values[column] = CStringGetTextDatum(row->region); break;
            case 3: slot->tts_values[column] = Int32GetDatum(row->amount); break;
            case 4: slot->tts_values[column] = CStringGetTextDatum(row->owner); break;
            case 5: slot->tts_values[column] = CStringGetTextDatum(row->card); break;
            default: ereport(ERROR, (errmsg("Unsupported FGAC scan attribute %d", source_attno)));
        }
    }
    ExecStoreVirtualTuple(slot); return slot;
}

static bool recheck_remote_tuple(ScanState *node, TupleTableSlot *slot)
{
    return true;
}

static TupleTableSlot *exec_custom_scan(CustomScanState *node)
{
    return ExecScan(&node->ss, fetch_remote_tuple, recheck_remote_tuple);
}
static void end_custom_scan(CustomScanState *node) {}
static void rescan_custom_scan(CustomScanState *node) { ((FgacScanState *) node)->cursor = 0; }
static void explain_custom_scan(CustomScanState *node, List *ancestors, ExplainState *es)
{
    CustomScan *scan = (CustomScan *) node->ss.ps.plan;
    char *remote_filter = strVal(lsecond(scan->custom_private));
    ExplainPropertyText("Remote Filter", remote_filter[0] ? remote_filter : "none", es);
    ExplainPropertyBool("Local Recheck", scan->scan.plan.qual != NIL, es);
    ExplainPropertyText("Remote Execution", "Once per CustomScan initialization", es);
    ExplainPropertyText("Materialization", "Query-scoped local buffer", es);
    ExplainPropertyText("Rescan Source", "Local materialization", es);
    ExplainPropertyBool("Parameterized", false, es);
}

void _PG_init(void)
{
    DefineCustomStringVariable("fgac.catalog_host", "Catalog DNS name", NULL, &catalog_host,
        "polaris-fgac", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("fgac.catalog_port", "Catalog port", NULL, &catalog_port,
        8181, 1, 65535, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("fgac.remote_host", "Remote DNS name", NULL, &remote_host,
        "adapter-core", PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("fgac.remote_port", "Remote port", NULL, &remote_port,
        8004, 1, 65535, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("fgac.user_token", "Query delegation token", NULL, &user_token,
        "", PGC_USERSET, GUC_NO_SHOW_ALL, NULL, NULL, NULL);
    RegisterCustomScanMethods(&scan_methods);
    previous_planner_hook = planner_hook; planner_hook = fgac_planner;
    previous_process_utility_hook = ProcessUtility_hook;
    ProcessUtility_hook = fgac_process_utility;
    previous_set_rel_pathlist_hook = set_rel_pathlist_hook; set_rel_pathlist_hook = fgac_set_rel_pathlist;
}

void _PG_fini(void)
{
    planner_hook = previous_planner_hook;
    ProcessUtility_hook = previous_process_utility_hook;
    set_rel_pathlist_hook = previous_set_rel_pathlist_hook;
}
