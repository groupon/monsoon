typedef hyper timestamp_msec;
typedef string string_val<>;  /* makes working with lists of strings easier */
typedef int timestamp_delta<>;  /* delta encoding: each value is difference (msec) since predecessor. */

/*
 * Bitset.
 *
 * Each index *i* encodes the number of booleans that follow after the previous one.
 * The value of the booleans described by index *i* is the inverse of the previous.
 * Index 0 uses a boolean value of *true*.
 *
 * Example decoder:
 *     bool[] decode(bitset values) {
 *       int len = sum(values);
 *       bool decoded[] = new bool[len];
 *
 *       int di = 0;  // Write index into decoded[].
 *       bool dv = true;  // Next to-be-written boolean value.
 *       for (int i = 0; i < values.length; ++i) {
 *         int dlen = values[i];
 *         for (int j = 0; j < dlen; ++j)
 *           decoded[di++] = dv;
 *
 *         di = !di;  // Flip for next value.
 *       }
 *       return decoded;
 *     }
 */
typedef unsigned short bitset<>;  /* bitset encoding */

enum metrickind {
    BOOL = 0,
    INT = 1,
    FLOAT = 2,
    STRING = 3,
    HISTOGRAM = 4,
    EMPTY = 0x7fffffff
};

enum header_flags {
    /* indicate if the file is a list or a tables kind of file. */
    KIND_MASK = 0xf,
    KIND_LIST = 0x0,
    KIND_TABLES = 0x1,

    /* indicate if opaque segments are compressed (gzip) */
    GZIP = 0x20000000,

    /* indicate if all records are sorted by timestamp */
    SORTED = 0x40000000,

    /* indicate if all records have distinct timestamps */
    DISTINCT = 0x80000000
};

struct histogram_entry {
    double floor;
    double ceil;
    double events;
};
typedef histogram_entry histogram<>;

union metric_value switch(metrickind kind) {
case BOOL:
    bool bool_value;
case INT:
    hyper int_value;
case FLOAT:
    double dbl_value;
case STRING:
    int str_dict_ref;
case HISTOGRAM:
    histogram hist_value;
case EMPTY:
    /* skip */
};

struct record_metrics {
    int path_ref;
    metric_value v;
};
struct record_tags {
    int tag_ref;
    record_metrics metrics<>;
};
struct record {
    int path_ref;
    record_tags tags<>;
};
typedef record record_array<>;

struct strval_dictionary_delta {
    int offset;
    string_val values<>;
};
typedef int path<>;
struct path_dictionary_delta {
    int offset;
    path values<>;  /* reference string dictionary */
};
struct tags {
    int str_ref<>;  /* reference string dictionary */
    metric_value value<>;
};
struct tag_dictionary_delta {
    int offset;
    tags values<>;
};
struct dictionary_delta {
    strval_dictionary_delta sdd;  /* referenced by other dictionary items as well */
    path_dictionary_delta pdd;  /* paths, for both group names and metric names */
    tag_dictionary_delta tdd;  /* tags */
};

/*
 * Followed by:
 * - crc32 (4 bytes, validates this record)
 * - dictionary delta (segment, dd_len bytes, omitted if dd_len=0)
 * - record_array (segment, r_len bytes)
 */
struct tsdata {
    timestamp_msec ts;
    hyper dd_len;
    hyper r_len;
    int reserved;  // Pads to multiple of 8 bytes, once CRC following this is considered.
};

struct file_data_tables {
    timestamp_delta tsd;
    dictionary_delta dictionary;  /* complete dictionary */
    tables tables_data;
};

/*
 * Pointer to file segment.
 *
 * A file segment is a block in the file.
 * It starts at the given 'offset' (bytes from begin for file).
 * The file segment contains 'len' bytes of data.
 * If the compress bit is specified, this data will be the length after compression.
 *
 * Following the data, between 0 and 3 padding bytes will exist, such that:
 *   (padlen + 'len') % 4 == 0
 *
 * After the padding, a 4 byte CRC32 is written in BIG ENDIAN (xdr int).
 * The CRC32 is calculated over the data and the padding bytes.
 */
struct file_segment {
    hyper offset;  /* file data offset */
    hyper len;  /* file data length */
};

struct tables_tag {
    int tag_ref;  /* reference to dictionary tags */
    file_segment pos;  /* references group table */
};
/* table offset map for all data in the file */
struct tables_group {
    int group_ref;  /* reference to dictionary path */
    tables_tag tag_tbl<>;
};

typedef tables_group tables<>;

struct tables_metric {
    int metric_ref;  /* reference to dictionary path */
    file_segment pos;  /* reference to metric table */
};
struct group_table {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    tables_metric metric_tbl<>;
};

struct mt_bool {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    bitset values;  /* values, only emitted for presence is true. */
};
struct mt_16bit {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    short values<>;  /* values are only emitted for presence is true. */
};
struct mt_32bit {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    int values<>;  /* values are only emitted for presence is true. */
};
struct mt_64bit {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    hyper values<>;  /* values are only emitted for presence is true. */
};
struct mt_dbl {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    double values<>;  /* values are only emitted for presence is true. */
};
struct mt_str {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    int values<>;  /* reference to string table, only emitted for presence is true. */
};
struct mt_hist {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    histogram values<>;  /* values are only emitted for presence is true. */
};
struct mt_empty {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    /* no values */
};
struct mt_other {
    bitset presence;  /* bitset indices correspond to indices in main timestamp array. */
    metric_value values<>;  /* values are only emitted for presence is true. */
};
/* encode all metric values in a set */
struct metric_table {
    mt_bool  metrics_bool;
    mt_16bit metrics_16bit;
    mt_32bit metrics_32bit;
    mt_64bit metrics_64bit;
    mt_dbl   metrics_dbl;
    mt_str   metrics_str;
    mt_hist  metrics_hist;
    mt_empty metrics_empty;
    mt_other metrics_other;  /* Allow for future expansion of types. */
};

struct tsfile_header {
    timestamp_msec first;
    timestamp_msec last;
    int flags;
    int reserved;  /* reserved for future use */
    hyper file_size;  /* file size */
    file_segment fdt;  /* file data tables (only if tables file) */
};
