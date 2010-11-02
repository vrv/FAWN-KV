#include "FawnKV.h"
#include "TFawnKVRemote.h"

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

void usage()
{
    printf("Usage: ./tester_remote [-c clientIP] frontendIP [fePort]\n");
}

int main(int argc, char **argv)
{
    extern char *optarg;
    extern int optind;
    string myIP = "";
    int myPort;
    int ch;
    int port = 4001;
    while ((ch = getopt(argc, argv, "c:p:")) != -1) {
        switch (ch) {
        case 'c':
            myIP = optarg;
	    break;
	case 'p':
	    myPort = atoi(optarg);
	    break;
	default:
            usage();
            exit(-1);
        }
    }

    argc -= optind;
    argv += optind;

    if (argc < 1) {
	usage();
	exit(-1);
    } else if (argc == 2) {
	port = atoi(argv[1]);
    }

    FawnKVClt client(argv[0], port, myIP, myPort);

    for (int i = 0; i < 10000; i++) {
	printf("putting..");
	client.put("abc", "value");
	string value = client.get("abc");
	printf("%s\n", value.c_str());
    }
    return 0;
}
