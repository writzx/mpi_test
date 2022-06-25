//
// Created by Writwick Das on 21/06/2022.
//

#include <sstream>
#include <algorithm>

#include "Executor.h"

using namespace std;

string Executor::get_row(Executor *executor, int row) {
    int rank = row / executor->N1;

    return "some row value";
}

string Executor::get_aggr(Executor *executor, int row) {
    return "some aggr value";
}

string Executor::execute_command(const string &command) {
    stringstream res_str;
    istringstream string_stream(command);

    string op, sub_op, row_str;

    // split the command into op, sub op and row index strings
    getline(string_stream, op, ' ');
    getline(string_stream, sub_op, ' ');
    getline(string_stream, row_str, ' ');

    if (op.substr(0, 4) == "exit") {
        res_str << "exited";
        return res_str.str();
    }

    // try to find operator in operator map keys
    auto op_element = op_map.find(op);
    if (op_element == op_map.end()) {
        // operator is not valid
        res_str << "error: operator \"" << op << "\" is invalid.";
        return res_str.str();
    }

    const auto sub_op_map = op_element->second;

    auto sub_op_element = sub_op_map.find(sub_op);
    if (sub_op_element == sub_op_map.end()) {
        // sub-operator is not valid for operator
        res_str << "error: sub operator \"" << sub_op << "\" is invalid for operator \"" << op << "\".";
        return res_str.str();
    }

    const auto op_func = sub_op_element->second;

    // try to parse the row value into integer
    stringstream row_stream(row_str);
    int row(0);
    row_stream >> row;

    if (row_stream.fail()) {
        return "error: failed to parse row index.";
    }

    // call the op_func to execute the command
    return op_func(this, row);
}

int Executor::parse_target_row(const string &command) {
    istringstream string_stream(command);

    string op, sub_op, row_str;

    // split the command into op, sub op and row index strings
    getline(string_stream, op, ' ');
    getline(string_stream, sub_op, ' ');
    getline(string_stream, row_str, ' ');

    // try to parse the row value into integer
    stringstream row_stream(row_str);
    int row(-1);
    row_stream >> row;

    if (row_stream.fail()) {
        return -1;
    }

    return row;
}