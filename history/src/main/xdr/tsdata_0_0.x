typedef string path_elem<>;

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

typedef hyper timestamp_msec;

union metric_value switch(metrickind kind) {
case BOOL:
    bool bool_value;
case INT:
    hyper int_value;
case FLOAT:
    double dbl_value;
case STRING:
    string str_value<>;
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

struct tsfile_metric {
    path metric;
    metric_value value;
};

struct tag_elem {
    string key<>;
    metric_value value;
};

struct tags {
    tag_elem data<>;
};

struct tsfile_tagged_datapoint {
    tags tags;
    tsfile_metric tsv<>;
};

struct tsfile_pathgroup {
    path group;
    tsfile_tagged_datapoint dps<>;
};

struct tsfile_datapoint {
    timestamp_msec ts;
    tsfile_pathgroup groups<>;
};

struct tsfile_header {
    timestamp_msec first;
    timestamp_msec last;
};
