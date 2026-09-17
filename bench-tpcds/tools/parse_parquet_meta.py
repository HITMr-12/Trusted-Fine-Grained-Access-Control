"""Minimal Thrift-compact Parquet footer parser (no external deps).

Parses FileMetaData enough to answer:
  - writer (created_by)
  - row groups / rows
  - per-column-chunk: path, encodings, RG-level Statistics (min/max/nulls)
  - ColumnIndex / OffsetIndex presence (column_index_offset etc.)
  - BloomFilter offset/length presence
Usage: python parse_parquet_meta.py <file.parquet | meta.bin>
"""
import sys, struct

STOP, BOOL_TRUE, BOOL_FALSE, BYTE, I16, I32, I64, DOUBLE, BINARY, LIST, SET, MAP, STRUCT = range(13)


class P:
    def __init__(self, b):
        self.b = b
        self.i = 0

    def rb(self):
        v = self.b[self.i]
        self.i += 1
        return v

    def varint(self):
        r = 0
        sh = 0
        while True:
            x = self.rb()
            r |= (x & 0x7f) << sh
            if not x & 0x80:
                return r
            sh += 7

    def zig(self, n):
        return (n >> 1) ^ -(n & 1)

    def int(self):
        return self.zig(self.varint())

    def binary(self):
        n = self.varint()
        v = self.b[self.i:self.i + n]
        self.i += n
        return v

    def field(self):
        """Read one field begin; returns (fid, type) or (None, STOP)."""
        b = self.rb()
        t = b & 0x0f
        if t == STOP:
            return None, STOP
        d = b >> 4
        fid = self.int() if d == 0 else None
        if d:
            fid = self._last + d
        self._last = fid
        return fid, t

    _last = 0

    def skip(self, t):
        if t in (BOOL_TRUE, BOOL_FALSE, STOP):
            return
        if t == BYTE:
            self.rb()
            return
        if t in (I16, I32, I64):
            self.varint()
            return
        if t == DOUBLE:
            self.i += 8
            return
        if t == BINARY:
            n = self.varint()
            self.i += n
            return
        if t in (LIST, SET):
            h = self.rb()
            n = h >> 4
            et = h & 0x0f
            if n == 15:
                n = self.varint()
            for _ in range(n):
                self.skip(et)
            return
        if t == MAP:
            n = self.varint()
            if n:
                h = self.rb()
                kt = h >> 4
                vt = h & 0x0f
                for _ in range(n):
                    self.skip(kt)
                    self.skip(vt)
            return
        if t == STRUCT:
            self.struct(lambda fid, t: self.skip(t))
            return
        raise ValueError('badtype %d at %d' % (t, self.i))

    def struct(self, hdl):
        """Iterate fields of a struct. hdl(fid, type) must consume the value."""
        prev = self._last
        self._last = 0
        try:
            while True:
                fid, t = self.field()
                if t == STOP:
                    return
                hdl(fid, t)
        finally:
            self._last = prev


def parse_footer(meta):
    out = {'rgs': 0, 'colinfo': []}
    p = P(meta)

    def filemeta(fid, t):
        if fid == 1:
            out['version'] = p.int()
        elif fid == 2:
            p.skip(t)  # schema
        elif fid == 3:
            out['num_rows'] = p.int()
        elif fid == 4:
            h = p.rb()
            n = h >> 4
            et = h & 0x0f
            if n == 15:
                n = p.varint()
            for _ in range(n):
                parse_rowgroup(p, out)
        elif fid == 6:
            out['created_by'] = p.binary().decode('utf8', 'replace')
        else:
            p.skip(t)

    p.struct(filemeta)
    return out


def parse_rowgroup(p, out):
    cols = []

    def rg(fid, t):
        if fid == 1:
            h = p.rb()
            n = h >> 4
            if n == 15:
                n = p.varint()
            for _ in range(n):
                cols.append(parse_colchunk(p))
        elif fid == 3:
            out.setdefault('rows_per_rg', []).append(p.int())
        else:
            p.skip(t)

    p.struct(rg)
    out['rgs'] += 1
    out['colinfo'].append(cols)


def parse_colchunk(p):
    info = {}

    def cc(fid, t):
        if fid == 3:
            info['meta'] = parse_colmeta(p)
        elif fid == 4:
            info['oi_off'] = p.int()
        elif fid == 5:
            info['oi_len'] = p.int()
        elif fid == 6:
            info['ci_off'] = p.int()
        elif fid == 7:
            info['ci_len'] = p.int()
        else:
            p.skip(t)

    p.struct(cc)
    return info


def parse_colmeta(p):
    m = {}

    def cm(fid, t):
        if fid == 3:
            h = p.rb()
            n = h >> 4
            if n == 15:
                n = p.varint()
            m['path'] = '.'.join(p.binary().decode('utf8', 'replace') for _ in range(n))
        elif fid == 2:
            h = p.rb()
            n = h >> 4
            if n == 15:
                n = p.varint()
            m['encodings'] = [p.int() for _ in range(n)]
        elif fid == 12:
            m['stats'] = parse_stats(p)
        elif fid in (14, 15):
            m['bf'] = p.int()
        else:
            p.skip(t)

    p.struct(cm)
    return m


def parse_stats(p):
    st = {}

    def ss(fid, t):
        if fid in (5, 6):
            st['minv' if fid == 5 else 'maxv'] = p.binary()
        elif fid == 7:
            st['nulls'] = p.int()
        else:
            p.skip(t)

    p.struct(ss)
    return st


if __name__ == '__main__':
    fn = sys.argv[1]
    if fn.endswith('.parquet'):
        with open(fn, 'rb') as f:
            f.seek(-8, 2)
            tail = f.read()
            mlen = struct.unpack('<i', tail[:4])[0]
            f.seek(-(8 + mlen), 2)
            meta = f.read(mlen)
    else:
        meta = open(fn, 'rb').read()
    out = parse_footer(meta)
    print('created_by:', out.get('created_by'))
    print('num_rows:', out.get('num_rows'), 'row_groups:', out['rgs'], 'rows_per_rg:', out.get('rows_per_rg'))
    tot = sum(len(rg) for rg in out['colinfo'])
    ci = sum(1 for rg in out['colinfo'] for c in rg if 'ci_off' in c)
    bf = sum(1 for rg in out['colinfo'] for c in rg if 'bf' in c.get('meta', {}))
    st_ = sum(1 for rg in out['colinfo'] for c in rg if c.get('meta', {}).get('stats'))
    print('ColumnIndex/OffsetIndex: %d/%d chunks, BloomFilter: %d/%d, RG-level stats: %d/%d' % (ci, tot, bf, tot, st_, tot))
    ENC = {0: 'PLAIN', 2: 'PLAIN_DICTIONARY', 3: 'RLE', 4: 'BIT_PACKED', 5: 'DELTA_BINARY_PACKED', 6: 'DELTA_LENGTH_BYTE_ARRAY', 7: 'DELTA_BYTE_ARRAY', 8: 'RLE_DICTIONARY', 9: 'BYTE_STREAM_SPLIT'}
    for c in out['colinfo'][0]:
        m = c.get('meta', {})
        t = m.get('stats', {})
        print('  col=%-16s enc=%-30s min_len=%-3s max_len=%-3s nulls=%-4s CI=%s BF=%s' % (
            m.get('path'),
            ','.join(ENC.get(e, str(e)) for e in m.get('encodings', [])),
            len(t.get('minv', b'')), len(t.get('maxv', b'')), t.get('nulls'),
            'ci_off' in c, 'bf' in m))
