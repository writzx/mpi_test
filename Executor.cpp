//
// Created by Writwick Das on 21/06/2022.
//

#include <sstream>
#include <algorithm>

#include "Executor.h"

using namespace std;

void Executor::get_func(Executor * executor, string sub_op, int row) {
    int rows_per_rank = executor->total_rows / executor->total_ranks;
    int rank = row / rows_per_rank;

    cout << "Rank: " << rank << endl;
}

void Executor::execute_command(const string &command) {
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
        cout << "Error: operator \"" << op << "\" is invalid." << endl;
        return;
    }

    const auto op_func = op_element->second.first;
    const auto allowed_sub_ops = op_element->second.second;

    // try to find sub operator in allowed sub operators list
    if (find(allowed_sub_ops.begin(), allowed_sub_ops.end(), sub_op) == allowed_sub_ops.end()) {
        // sub-operator is not valid for operator
        cout << "Error: sub operator \"" << sub_op << "\" is invalid for operator \"" << op << "\"." << endl;
        return;
    }

    // try to parse the row value into integer
    stringstream row_stream(row_str);
    int row(0);
    row_stream >> row;

    if (row_stream.fail()) {
        cout << "Error: failed to parse row index." << endl;
        return;
    }

    // call the op_func to execute the command
    op_func(this, sub_op, row);
}