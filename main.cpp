#include <mpi.h>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <future>
#include <cmath>

#include "Executor.h"

using namespace std;

Executor *executor;

vector<string> read_commands(const string &path);

void mpi_loop();

void validate_and_execute(const string &command, int N, int N1, int rank_count);

int main(int argc, char **argv) {
    if (argc < 2) {
        cout << "error: total number of rows not specified." << endl;
        return 0;
    }

    if (argc < 3) {
        cout << "error: total number of cols not specified." << endl;
        return 0;
    }

    // try to register for multi-thread mpi
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    if (provided != MPI_THREAD_MULTIPLE) {
        // if failed to register for multi-threading, show an error message and quit
        cout << "error: couldn't register mpi for multi-thread operations." << endl;
        MPI_Finalize();
        return 0;
    }

    int total_rank; // #n
    MPI_Comm_size(MPI_COMM_WORLD, &total_rank);

    int current_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &current_rank);

    // try to parse N from command line
    stringstream bigN_str(argv[1]);
    int bigN(0);
    bigN_str >> bigN;

    if (bigN_str.fail()) {
        cout << "error: invalid value for N." << endl;
        return 0;
    }

    // try to parse M from command line
    stringstream bigM_str(argv[2]);
    int bigM(0);
    bigM_str >> bigM;

    if (bigM_str.fail()) {
        cout << "error: invalid value for M." << endl;
        return 0;
    }

    // calculate N1 (average rows per rank)
    int N1 = ceil((float) bigN / total_rank);

    // allocate only remaining rows if it's the last rank
    if (current_rank == total_rank - 1) {
        N1 = bigN - (total_rank - 1) * N1;
    }

    // initialize executor for all ranks including 0
    executor = new Executor(current_rank, bigN, bigM, N1, total_rank);

    // start the mpi loop asynchronously for all ranks including 0
    auto task = async(mpi_loop);

    // only allow running commands if rank is 0
    if (current_rank == 0) {
        if (argc < 4) {
            string command;
            do {
                // read commands and execute them until "exit" is called
                getline(cin, command);
                validate_and_execute(command, bigN, N1, total_rank);
            } while (command != "exit");
        } else {
            auto commands = read_commands(argv[3]);
            bool exited = false;

            // read commands and execute until end of file or until we reach an "exit" call
            for (const string &command: commands) {
                validate_and_execute(command, bigN, N1, total_rank);
                if (command.substr(0, 4) == "exit") {
                    exited = true;
                    break;
                }
            }

            if (!exited) {
                // if exit was not called, call it once
                validate_and_execute("exit", bigN, N1, total_rank);
            }
        }
    }

    // wait for mpi loop to exit
    task.wait();

    MPI_Finalize();

    return 0;
}

// executes remote commands and prints their results
void execute_remote_command(int rank, const string &command) {
    // send the command to the target rank
    cout << "rank " << rank << " << " << command << endl;
    MPI_Send(command.c_str(), command.length(), MPI_CHAR, rank, 0, MPI_COMM_WORLD);

    int result_len;
    MPI_Status status;

    // receive the result length then receive the result value and print it
    MPI_Probe(rank, 0, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_INT, &result_len);


    vector<int> result;
    result.resize(result_len);

    MPI_Recv(&result[0], result_len, MPI_INT, rank, 0,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (result_len == 1) {
        // check if the result length is 1, print as single int value
        if (result[0] > -1) {
            // if result value is -1 or lower, it means error so don't print
            // anything since the error is already printed by the other rank
            cout << "rank " << rank << " >> " << result[0] << endl;
        }
    } else {
        stringstream res_stream;

        // format as an array e.g.: { 1, 2, ... }
        string sep = "{ ";
        for (const auto &r: result) {
            res_stream << sep << r;
            sep = ", ";
        }

        res_stream << " }";

        // print the formatted array as output
        cout << "rank " << rank << " >> " << res_stream.str() << endl;
    }
}

// validates commands then executes them
void validate_and_execute(const string &command, int N, int N1, int rank_count) {
    int target_row = Executor::parse_command(command);
    int target_rank = (target_row / N1);

    // operator is empty, ignore
    if (target_row == -1) {
        return;
    }

    // special operator
    // it should be sent to all ranks
    if (target_row == -2) {
        for (int i = 0; i < rank_count; ++i) {
            execute_remote_command(i, command);
        }
        return;
    }

    // otherwise just show error message
    if (target_row == -3) {
        cout << "error: invalid command or arguments." << endl;
        return;
    }

    if (target_rank >= rank_count || target_row >= N) {
        cout << "error: invalid row input: " << target_row << " (inferred rank: " << target_rank
             << " is invalid. valid row value range: [0, "
             << N - 1 << "])" << endl;
        return;
    }

    execute_remote_command(target_rank, command);
}

// the basic mpi loop for receiving and executing commands then sending back results
void mpi_loop() {
    string command;
    do {
        int command_len;
        MPI_Status status;

        // receive the command length then receive the command
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &command_len);

        command.resize(command_len);

        MPI_Recv(&command[0], command_len, MPI_CHAR, 0, 0,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // run the command send the result back
        int count;
        auto result = executor->execute_command(command, count);

        MPI_Send(result, count, MPI_INT, 0, 0, MPI_COMM_WORLD);
    } while (command.substr(0, 4) != "exit");
}

// reads all commands from file
vector<string> read_commands(const string &path) {
    ifstream file_stream(path);
    string command;
    vector<string> commands;

    while (getline(file_stream, command)) {
        commands.push_back(command);
    }

    return commands;
}
