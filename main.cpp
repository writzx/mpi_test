#include <mpi.h>
#include <algorithm>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <map>

using namespace std;

// TODO take this as input somehow
int big_N = 300;

// TODO define as a class field
int total_rank; // #n

void get_func(string sub_op, int row) {
    int rows_per_rank = big_N / total_rank;
    int rank = row / rows_per_rank;

    cout << "Rank: " << rank << endl;
}

// holds the operators, functions and sub operators
const map<string, pair<void (*)(string, int), vector<string>>> op_map{
        {"get", {get_func, {"row", "aggr"}}}
};

void execute_command(const string &command) {
    istringstream string_stream(command);

    string op, sub_op, row_str;

    // split the command into op, sub op and row index strings
    getline(string_stream, op, ' ');
    getline(string_stream, sub_op, ' ');
    getline(string_stream, row_str, ' ');

    // try to find operator in operator map keys
    auto op_element = op_map.find(op);
    if (op_element == op_map.end()) {
        // operator is not valid
        cout << "operator \"" << op << "\" is invalid." << endl;
        return;
    }

    const auto op_func = op_element->second.first;
    const auto allowed_sub_ops = op_element->second.second;

    // try to find sub operator in allowed sub operators list
    if (find(allowed_sub_ops.begin(), allowed_sub_ops.end(), sub_op) == allowed_sub_ops.end()) {
        // sub-operator is not valid for operator
        cout << "sub operator \"" << sub_op << "\" is invalid for operator \"" << op << "\"." << endl;
        return;
    }

    // try to parse the row value into integer
    stringstream row_stream(row_str);
    int row(0);
    row_stream >> row;

    if (row_stream.fail()) {
        cout << "failed to parse row index." << endl;
        return;
    }

    // call the op_func to execute the command
    op_func(sub_op, row);
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

    MPI_Comm_size(MPI_COMM_WORLD, &total_rank);

    int current_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &current_rank);

    if (current_rank == 0) {
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
    }

    MPI_Finalize();

    return 0;
}