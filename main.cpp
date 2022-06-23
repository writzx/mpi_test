#include <mpi.h>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <future>

#include "Executor.h"

using namespace std;

vector<string> read_commands(const string &path);

void mpi_loop(int rank);

void send_command(string command);

Executor *executor;

int main(int argc, char **argv) {
    if (argc < 2) {
        cout << "Error: total number of rows not specified." << endl;
        return 0;
    }

    if (argc < 3) {
        cout << "Error: total number of cols not specified." << endl;
        return 0;
    }

    MPI_Init(&argc, &argv);

    int total_rank; // #n
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

    // try to parse N from command line
    stringstream bigM_str(argv[2]);
    int bigM(0);
    bigM_str >> bigM;

    if (bigM_str.fail()) {
        cout << "Error: invalid value for M." << endl;
        return 0;
    }

    // calculate N1
    int N1 = bigN / total_rank;

    *executor = Executor(bigN, bigM, N1, total_rank);

    // allocate array N1 x M;
    vector<vector<int>> array_part(executor->N1, vector<int>(executor->M, current_rank));

    auto task = async(mpi_loop, current_rank);

    // only allow running commands if rank is 0
    if (current_rank == 0) {
        if (argc < 4) {
            string command;
            do {
                getline(cin, command);
                send_command(command);
            } while (command != "exit");
        } else {
            auto commands = read_commands(argv[3]);
            for (const string &command: commands) {
                send_command(command);
                if (command == "exit") {
                    break;
                }
            }
        }
    }

    task.wait();

    MPI_Finalize();

    return 0;
}

void send_command(string command) {
    int target_rank = executor->parse_target_row(command) / executor->N1;
}

void mpi_loop(int rank) {
    string command;
    do {
    } while (command != "exit");
}

vector<string> read_commands(const string &path) {
    ifstream file_stream(path);
    string command;
    vector<string> commands;

    while (getline(file_stream, command)) {
        commands.push_back(command);
    }

    return commands;
}
