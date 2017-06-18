typedef string path_elem<>;
typedef hyper timestamp_msec;
typedef hyper duration_msec;

struct path {
    path_elem elems<>;
};

struct literals_tag {
    string name<>;
    literals_metric_value value;
};

struct literals_group_name {
    path group_path;
    literals_tag tags<>;
};

enum metrickind {
    BOOL = 0,
    INT = 1,
    FLOAT = 2,
    STRING = 3,
    HISTOGRAM = 4,
    EMPTY = 0x7fffffff
};

enum iter_result_code {
    SUCCESS = 0,
    UNKNOWN_ITER = 1
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

union literals_metric_value switch(metrickind kind) {
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

struct tsfile_record_entry {
    int metric_ref;
    metric_value v;
};

struct tsfile_record {
    int group_ref;
    int tag_ref;
    tsfile_record_entry metrics<>;
};

struct timestamped_tsfile_record {
    timestamp_msec ts;
    dictionary_delta *dd;
    tsfile_record record;
};

struct timeseries_collection {
    timestamp_msec ts;
    dictionary_delta *dd;
    tsfile_record records<>;
};

struct list_of_timeseries_collection {
    timeseries_collection collections<>;
};

struct list_of_timestamped_tsfile_record {
    timestamped_tsfile_record records<>;
};

struct tagged_metric_value {
    int tag_ref;
    metric_value v;
};

union timeseries_metric_delta_set switch(bool is_vector) {
case FALSE:
    metric_value scalar;
case TRUE:
    tagged_metric_value vector<>;
};

struct named_evaluation_map_entry {
    string name<>;
    string expr<>;
};

struct named_evaluation_map {
    named_evaluation_map_entry entries<>;
};

struct named_evaluation {
    string name<>;
    timestamp_msec ts;
    timeseries_metric_delta_set ts_set;
};

struct named_evaluation_set {
    dictionary_delta *dd;
    named_evaluation entries<>;
};

struct stream_iter_tsc_response_success {
    hyper cookie;
    bool last;
    list_of_timeseries_collection rv;
};

union stream_iter_tsc_response switch(iter_result_code result) {
case SUCCESS:
    stream_iter_tsc_response_success response;
case UNKNOWN_ITER:
    void;
};

struct group_stream_iter_response_success {
    hyper cookie;
    bool last;
    list_of_timestamped_tsfile_record rv;
};

union group_stream_iter_response switch(iter_result_code result) {
case SUCCESS:
    group_stream_iter_response_success response;
case UNKNOWN_ITER:
    void;
};

struct stream_response {
    hyper iter_id;
    stream_iter_tsc_response_success first_response;
};

struct evaluate_iter_response_success {
    hyper cookie;
    bool last;
    named_evaluation_set rv<>;
};

union evaluate_iter_response switch(iter_result_code result) {
case SUCCESS:
    evaluate_iter_response_success response;
case UNKNOWN_ITER:
    void;
};

struct evaluate_response {
    hyper iter_id;
    evaluate_iter_response_success first_response;
};

struct group_stream_response {
    hyper iter_id;
    group_stream_iter_response_success first_response;
};


program rhistory {
    version vers {
        bool addTSData(list_of_timeseries_collection) = 1;
        hyper getFileSize(void) = 2;
        timestamp_msec getEnd(void) = 3;

        stream_iter_tsc_response streamIterTscNext(hyper, hyper, int) = 100;
        void closeIterTsc(hyper, hyper) = 101;
        stream_response streamReverse(int) = 110;
        stream_response streamReverseFrom(timestamp_msec, int) = 111;
        stream_response stream(int) = 120;
        stream_response streamFrom(timestamp_msec, int) = 121;
        stream_response streamFromTo(timestamp_msec, timestamp_msec, int) = 122;
        stream_response streamStepped(duration_msec, int) = 130;
        stream_response streamSteppedFrom(timestamp_msec, duration_msec, int) = 131;
        stream_response streamSteppedFromTo(timestamp_msec, timestamp_msec, duration_msec, int) = 132;

        evaluate_iter_response evaluateIterNext(hyper, hyper, int) = 200;
        void closeEvalIter(hyper, hyper) = 201;
        evaluate_response evaluate(named_evaluation_map, duration_msec, int) = 210;
        evaluate_response evaluateFrom(named_evaluation_map, timestamp_msec, duration_msec, int) = 211;
        evaluate_response evaluateFromTo(named_evaluation_map, timestamp_msec, timestamp_msec, duration_msec, int) = 212;

        group_stream_iter_response streamGroupIterNext(hyper, hyper, int) = 300;
        void closeGroupIter(hyper, hyper) = 301;
        group_stream_response streamGroup(timestamp_msec, literals_group_name, int) = 310;
    } = 1;
} = 0x20131719;
