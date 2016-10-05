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

    /* indicate if opaque data is compressed (gzip) */
    GZIP = 0x20000000,

    /* indicate if the file contains sorted records */
    SORTED = 0x40000000,

    /* indicate if the file contains duplicate records */
    DUP_TS = 0x80000000
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

struct records {
    timestamp_msec ts;
    opaque dictionary_delta<>;  /* XDR encoded: dictionary delta, size = 0 indicates no dictionary update */
    opaque record_array<>;  /* XDR encoded: record_array */
};
struct file_data_list {
    records list_data<>;
};

struct file_data_tables {
    dictionary_delta dictionary;  /* complete dictionary */
    timestamp_delta tsd;
    tables tables_data;
};

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
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    tables_metric metric_tbl<>;
};

struct mt_bool {
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    bool values<>;
};
struct mt_16bit {
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    short values<>;
};
struct mt_32bit {
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    int values<>;
};
struct mt_64bit {
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    hyper values<>;
};
struct mt_dbl {
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    double values<>;
};
struct mt_str {
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    int values<>;  /* reference to string table */
};
struct mt_hist {
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    histogram values<>;
};
struct mt_empty {
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    /* no values */
};
struct mt_other {
    int timestamp_delta<>;  /* msec since begin of file, sufficient for encoding ~24 days */
    metric_value values<>;
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
    file_segment fdt;  /* file data tables (only if tables file) */
};
