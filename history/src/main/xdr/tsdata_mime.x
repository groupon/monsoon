const MAGIC_LEN = 12;

struct tsfile_mimeheader {
    opaque magic[MAGIC_LEN];
    int version_number;
};
