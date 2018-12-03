#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "hash.h"

#define MXU 300
#define TMC (1L<<24)
#define UMC (1L<<23)
#define VMC (1L<<20)
typedef unsigned long long ULL;

/*
char *strsep(char **stringp, const char *delim) {
    char *s;
    const char *spanp;
    int c, sc;
    char *tok;
    if ((s = *stringp)== NULL)
        return (NULL);
    for (tok = s;;) {
        c = *s++;
        spanp = delim;
        do {
            if ((sc =*spanp++) == c) {
                if (c == 0)
                    s = NULL;
                else
                    s[-1] = 0;
                *stringp = s;
                return (tok);
            }
        } while (sc != 0);
    }
}
*/

static inline int charmask(unsigned char *input, int len, char *mask) {
    unsigned char *end;
    unsigned char c;
    int result = 0;
    memset(mask, 0, 256);
    for (end = input + len; input < end; input++) {
        c=*input;
        if ((input + 3 < end) && input[1] == '.' && input[2] == '.'
                && input[3] >= c) {
            memset(mask + c, 1, input[3] - c + 1);
            input += 3;
        } else if ((input + 1 < end) && input[0] == '.' && input[1] == '.') {
            if (end-len >= input) { 
                result = -1;
                continue;
            }
            if (input + 2 >= end) { 
                result = -1;
                continue;
            }
            if (input[-1] > input[2]) { 
                result = -1;
                continue;
            }
            result = -1;
            continue;
        } else {
            mask[c] = 1;
        }
    }
    return result;
}

static char * trim(char *c, int mode){
    if (!c)
        return NULL;
    register int i;
    int len = strlen(c) + 1;
    int trimmed = 0;
    char mask[256];
    charmask((unsigned char*)" \n\r\t\v\0", 6, mask);
    if (mode & 1) {
        for (i = 0; i < len; i++) {
            if (mask[(unsigned char)c[i]]) {
                trimmed++;
            } else {
                break;
            }
        }
        len -= trimmed;
        c += trimmed;
    }
    if (mode & 2) {
        for (i = len - 1; i >= 0; i--) {
            if (mask[(unsigned char)c[i]]) {
                len--;
            } else {
                break;
            }
        }
    }
    c[len] = '\0';
    return c;
}

static void v2h(FILE *in, FILE *out){
    Hash * uhs = hash_create(UMC, STRING);
    Hash * vhs = hash_create(VMC, STRING);
    int user_ids_cnt = 0;
    int (*toks_st)[2] = (int(*)[2]) calloc(TMC, sizeof(int[2]));
    int *user_st = (int*) calloc(UMC, sizeof(int));
    int *user_ct = (int*) calloc(UMC, sizeof(int));
    int uid, iid, tk = 0;
    memset(user_st, -1, UMC * sizeof(int));
    char *string, *token;
    char buf[1024] = {0};
    while (NULL != fgets(buf, 1024, in)){
        string = trim(buf, 3);
        uid = hash_add(uhs, strsep(&string, "\t"));
        iid = hash_add(vhs, strsep(&string, "\t"));
        toks_st[tk][0] = iid;
        toks_st[tk][1] = user_st[uid];
        user_st[uid] = tk;
        user_ct[uid] += 1;
        tk += 1;
    }
    user_ids_cnt = hash_cnt(uhs);
    for (int u = 0; u < user_ids_cnt; u++){
        if (user_ct[u] > MXU){
            continue;
        }
        fprintf(out, "%s", hash_keystr(uhs, u));
        int p = user_st[u];
        while (p != -1){
            fprintf(out, "\t%s", hash_keystr(vhs, toks_st[p][0]));
            p = toks_st[p][1];
        }
        fprintf(out, "\n");
    }
}

 
// hash function for string 
static unsigned long long hash_func(char *arKey)
{
    register unsigned long long hash = 5381;
    int      nKeyLength = strlen(arKey);
    for (; nKeyLength >= 8; nKeyLength -= 8) {
        hash = ((hash << 5) + hash) + *arKey++;
        hash = ((hash << 5) + hash) + *arKey++;
        hash = ((hash << 5) + hash) + *arKey++;
        hash = ((hash << 5) + hash) + *arKey++;
        hash = ((hash << 5) + hash) + *arKey++;
        hash = ((hash << 5) + hash) + *arKey++;
        hash = ((hash << 5) + hash) + *arKey++;
        hash = ((hash << 5) + hash) + *arKey++;
    }
    switch (nKeyLength) {
        case 7: hash = ((hash << 5) + hash) + *arKey++; /* fallthrough... */
        case 6: hash = ((hash << 5) + hash) + *arKey++; /* fallthrough... */
        case 5: hash = ((hash << 5) + hash) + *arKey++; /* fallthrough... */
        case 4: hash = ((hash << 5) + hash) + *arKey++; /* fallthrough... */
        case 3: hash = ((hash << 5) + hash) + *arKey++; /* fallthrough... */
        case 2: hash = ((hash << 5) + hash) + *arKey++; /* fallthrough... */
        case 1: hash = ((hash << 5) + hash) + *arKey++; break;
        case 0: break;
        default:
                break;
    }
    return hash;
}

int main(int argc, char *argv[]) {
    int i;
    int mpcnt = atoi(argv[1]);  // split count for arrange
    int (*pipfds)[2] = (int(*)[2])calloc(mpcnt, sizeof(int[2]));
    pid_t *pids = (pid_t*)calloc(mpcnt, sizeof(pid_t));
    pid_t  pid;
    for (i = 0; i < mpcnt; i++){
        if(pipe(pipfds[i]) == -1){
            fprintf(stderr, "pipe create failed\n");
            return -1;
        }
    }
    for (i = 0; i < mpcnt; i++){
        pid = fork();
        if (pid == -1){
            break;
        }
        else if (pid == 0){
            pids[i] = getpid();
            break;
        }
        else{
            pids[i] = pid;
        }
    }
    if (pid == -1){
        fprintf(stderr, "process create failed\n");
        _exit(EXIT_FAILURE);
    }
    else if (pid == 0) {
        char  buf[1024] = {0};
        char  outs[100] = {0};
        int   c = 0, p = -1;
        pid_t self = getpid();
        for (c = 0; c < mpcnt ; c++){
            close(pipfds[c][1]);
            if (pids[c] == self){
                p = c;
            }
            else{
                close(pipfds[c][0]);
            }
        }
        if (p == -1){
            _exit(EXIT_FAILURE);
        }
        sprintf(outs, "%d.txt", p);
        FILE * inf = fdopen(pipfds[p][0], "r");
        FILE * fp = fopen(outs, "w");
        //read line by line
        v2h(inf, fp);
      //while(NULL != fgets(buf, 1024, inf)){
      //    fprintf(fp, "%s", buf);
      //    printf("%s", buf);
      //}
        fclose(fp);
        fclose(inf);
        close(pipfds[p][0]);
        _exit(EXIT_SUCCESS);
    }
    else { 
        for (int c = 0; c < mpcnt; c++){
            close(pipfds[c][0]);
        }
        char buf[1024] = {0};
        char out[1024] = {0};
        char *string, *token;
        while (NULL != fgets(buf, 1024, stdin)){
            string = trim(buf, 3);
            memmove(out, string, strlen(string) + 2);
            out[strlen(string)] = '\n';
            out[strlen(string) + 1] = 0;
            token       = strsep(&string, "\t");
            ULL sign    = hash_func(token);
            ULL m = sign % mpcnt;
            if (pids[m] == 0){
                fprintf(stderr, "subprocess create failed, just skip it\n");
            }
            else {
                write(pipfds[m][1], out, strlen(out));
            }
        }
        for (int c = 0; c < mpcnt; c++){
            close(pipfds[c][1]);
        }
        wait(NULL);
        exit(EXIT_SUCCESS);
    }
    for (int c = 0; c < mpcnt; c++){
        printf("close in main process: %d\n", pipfds[c][1]);
        close(pipfds[c][1]);
    }
    return 0;
}
