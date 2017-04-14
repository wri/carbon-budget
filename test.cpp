#include <map>
#include <iostream>
#include <stdlib.h>
using namespace std;



int main(int argc, char* argv[])
{

    int val1=atoi(argv[1]);
    int val2=atoi(argv[2]);

    cout << "input: " << val1 <<", "<< val2 << endl;

    typedef pair<unsigned int, unsigned int> pair_k;
    map<pair_k, float> mapping;

    mapping[make_pair(8,4)] = 25;
    mapping[make_pair(8,1)] = 31;
    mapping[make_pair(7,4)] = 39;
    mapping[make_pair(7,1)] = 55;
    mapping[make_pair(4,4)] = 28;
    mapping[make_pair(4,1)] = 27;
    mapping[make_pair(3,4)] = 16;
    mapping[make_pair(3,1)] = 26;
    mapping[make_pair(2,4)] = 28.2;
    mapping[make_pair(2,1)] = 20.3;
    mapping[make_pair(1,4)] = 13;
    mapping[make_pair(1,1)] = 22;
    mapping[make_pair(12,4)] = 2.8;
    mapping[make_pair(12,1)] = 4.1;
    mapping[make_pair(10,4)] = 2.1;
    mapping[make_pair(10,1)] = 5.2;

    float result;
    result = mapping[make_pair(val1,val2)];

    if (result == 0) {
        result = 255;}

    cout << result << endl;

    return 0;
}