# -*- coding: utf-8 -*-
"""FGAC 模式负向检查（纯 Flight 客户端，不经 Spark）：
  1) 无效 token                        -> 期望 Unauthenticated(401)
  2) plan 投影未授权列 ss_ticket_number -> 期望 Unauthorized(403)
"""
import json, os
import pyarrow.flight as flight

url = os.getenv("FGAC_FLIGHT_URL", "grpc://172.168.22.23:18833")
cases = json.load(open("/home/lyb/fgac-lab/deploy/fgac-tpcds-bench/cases.json"))
alice001 = next(c for c in cases if c["name"] == "alice_001")


def describe(plan, token):
    client = flight.FlightClient(url)
    try:
        options = flight.FlightCallOptions(timeout=30, headers=[(b"authorization", token.encode())])
        return client.get_flight_info(flight.FlightDescriptor.for_command(json.dumps(plan).encode()), options)
    except flight.FlightUnauthenticatedError:
        return "UNAUTHENTICATED"
    except flight.FlightUnauthorizedError as e:
        return "UNAUTHORIZED: " + str(e)[:120]
    except Exception as e:
        return "OTHER: " + type(e).__name__ + " " + str(e)[:120]
    finally:
        client.close()


r1 = describe(alice001["plan"], "Bearer nobody-token")
ok1 = r1 == "UNAUTHENTICATED"
print(("NEGATIVE_PASS " if ok1 else "NEGATIVE_FAIL ") + "无效token -> " + r1)

bad_plan = json.loads(json.dumps(alice001["plan"]))
bad_plan["root"]["columns"] = ["ss_item_sk", "ss_ticket_number"]
r2 = describe(bad_plan, "Bearer alice-token")
ok2 = r2.startswith("UNAUTHORIZED")
print(("NEGATIVE_PASS " if ok2 else "NEGATIVE_FAIL ") + "未授权列 -> " + r2)
