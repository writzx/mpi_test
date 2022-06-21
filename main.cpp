#include <mpi.h>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>

#include "Executor.h"

using namespace std;

vector<string> read_commands(const string &path) {
    ifstream file_stream(path);
    string command;
    vector<string> commands;

    while (getline(file_stream, command)) {
        commands.push_back(command);
    }

    return commands;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        cout << "Error: total number of rows not specified." << endl;
        return 0;
    }
    MPI_Init(&argc, &argv);

    int total_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &total_rank);

    int current_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &current_rank);

    // try to parse N from command line
    stringstream bigN_str(argv[1]);
    int bigN(0);
    bigN_str >> bigN;

    if (bigN_str.fail()) {
        cout << "Error: invalid value for N." << endl;
        return 0;
    }

    Executor exec(bigN, total_rank);

    if (current_rank == 0) {
        if (argc < 3) {
            string command;
            while (getline(cin, command)) {
                if (command == "exit")
                    break;
                exec.execute_command(command);
            }
        } else {
            auto commands = read_commands(argv[2]);
            for (const string &command: commands) {
                exec.execute_command(command);
            }
        }
    }

    MPI_Finalize();

    return 0;
}