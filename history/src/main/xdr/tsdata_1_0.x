typedef string path_elem<>;
typedef hyper timestamp_msec;

struct path {
    path_elem elems<>;
};

enum metrickind {
    BOOL = 0,
    INT = 1,
    FLOAT = 2,
    STRING = 3,
    HISTOGRAM = 4,
    EMPTY = 0x7fffffff
};

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
    histogram_entry hist_value<>;
case EMPTY:
    /* skip */
};

struct histogram_entry {
    double floor;
    double ceil;
    double events;
};

struct tag_elem {
    string key<>;
    metric_value value;
};

struct tags {
    tag_elem elems<>;
};

struct path_dictionary_delta {
    int id;
    path value;
};

struct tag_dictionary_delta {
    int id;
    tags value;
};

struct strval_dictionary_delta {
    int id;
    string value<>;
};

struct dictionary_delta {
    strval_dictionary_delta sdd<>;  /* may be referenced by tag strings */
    path_dictionary_delta gdd<>;
    path_dictionary_delta mdd<>;
    tag_dictionary_delta tdd<>;
};

struct tsfile_header {
    timestamp_msec first;
    timestamp_msec last;
};

struct tsfile_record_entry {
    int metric_ref;
    metric_value v;
};

struct tsfile_record {
    int group_ref;
    int tag_ref;
    tsfile_record_entry metrics<>;
};

struct tsfile_data {
    timestamp_msec ts;
    dictionary_delta *dd;
    tsfile_record records<>;
};
