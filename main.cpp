#include <mpi.h>
#include <iostream>
#include <vector>
#include <fstream>

using namespace std;

void execute_command(const string &command) {
    cout << command << endl;
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

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int total_rank; // #n
    MPI_Comm_size(MPI_COMM_WORLD, &total_rank);

    int current_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &current_rank);

    switch (current_rank) {
        case 0: // rank 0
            if (argc < 2) {
                string command;
                while (getline(cin, command)) {
                    if (command == "exit")
                        break;
                    execute_command(command);
                }
            } else {
                auto commands = read_commands(argv[1]);
                for (const string &command: commands) {
                    execute_command(command);
                }
            }
            break;
        default:
            break;
    }

    MPI_Finalize();

    return 0;
}