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

void mpi_loop(int rank);

void execute_remote_command(const string &command, int N1, int rank_count);

int main(int argc, char **argv) {
    if (argc < 2) {
        cout << "error: total number of rows not specified." << endl;
        return 0;
    }

    if (argc < 3) {
        cout << "error: total number of cols not specified." << endl;
        return 0;
    }

    MPI_Init(&argc, &argv);

    int total_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &total_rank);

    // exclude rank 0
    int n = total_rank - 1;

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

    // try to parse N from command line
    stringstream bigM_str(argv[2]);
    int bigM(0);
    bigM_str >> bigM;

    if (bigM_str.fail()) {
        cout << "error: invalid value for M." << endl;
        return 0;
    }

    // calculate N1 (average rows per rank)
    int N1 = ceil((float) bigN / n);

    // only allow running commands if rank is 0
    if (current_rank == 0) {
        if (argc < 4) {
            string command;
            do {
                getline(cin, command);
                execute_remote_command(command, N1, n);
            } while (command != "exit");
        } else {
            auto commands = read_commands(argv[3]);
            bool exited = false;
            for (const string &command: commands) {
                execute_remote_command(command, N1, n);
                if (command.substr(0, 4) == "exit") {
                    exited = true;
                    break;
                }
            }

            if (!exited) {
                // if exit was not called, call it once
                execute_remote_command("exit", N1, n);
            }
        }
    } else {
        // allocate only remaining rows if it's the last rank
        if (current_rank == n) {
            N1 = bigN - (n - 1) * N1;
        }

        executor = new Executor(current_rank, bigN, bigM, N1, n);

        auto task = async(mpi_loop, current_rank);
        task.wait();
    }

    MPI_Finalize();

    return 0;
}

void execute_remote_command(int rank, const string &command) {
    cout << "rank " << rank << " << " << command << endl;
    MPI_Send(command.c_str(), command.length(), MPI_CHAR, rank, 0, MPI_COMM_WORLD);

    int result_len;
    string result;
    MPI_Status status;

    MPI_Probe(rank, 0, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &result_len);

    result.resize(result_len);

    MPI_Recv(&result[0], result_len, MPI_CHAR, rank, 0,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    cout << "rank " << rank << " >> " << result << endl;
}

void execute_remote_command(const string &command, int N1, int rank_count) {
    int target_row = Executor::parse_target_row(command);
    int target_rank = 1 + (target_row / N1);

    if (target_row == -1) {
        for (int i = 0; i < rank_count; ++i) {
            execute_remote_command(i + 1, command);
        }
        return;
    }

    if (target_row == -2) {
        cout << "error: invalid command or arguments." << endl;
        return;
    }

    if (target_rank > rank_count) {
        cout << "error: invalid row input: " << target_row << " (inferred rank: " << target_rank
             << " is invalid. valid row value range: [0, "
             << N1 * rank_count - 1 << "])" << endl;
        return;
    }

    execute_remote_command(target_rank, command);
}

void mpi_loop(int rank) {
    string command;
    do {
        int command_len;
        MPI_Status status;

        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &command_len);

        command.resize(command_len);

        MPI_Recv(&command[0], command_len, MPI_CHAR, 0, 0,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        auto result = executor->execute_command(command);

        MPI_Send(result.c_str(), result.length(), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    } while (command.substr(0, 4) != "exit");
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
