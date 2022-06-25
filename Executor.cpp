//
// Created by Writwick Das on 21/06/2022.
//

#include <sstream>
#include <algorithm>
#include <cmath>

#include "Executor.h"

using namespace std;

// find the local row index in array_part from global row index
int Executor::get_local_row(int row) const {
    int global_N1 = ceil((float) this->N / this->rank_count);
    int local_row = row - (this->rank - 1) * global_N1;

    if (local_row < 0 || local_row > this->N1) {
        return -1;
    }

    return local_row;
}

// executes the command in local context and sends back the result
string Executor::execute_command(const string &command) {
    stringstream res_str;
    istringstream string_stream(command);

    string op, sub_op, row_str;

    // split the command into op, sub op and row index strings
    getline(string_stream, op, ' ');
    getline(string_stream, sub_op, ' ');
    getline(string_stream, row_str, ' ');

    // try to find operator in special operator map keys
    auto sp_op_element = Executor::special_op_map.find(op);
    if (sp_op_element != Executor::special_op_map.end()) {
        // it's a valid special operator so return its result directly
        return sp_op_element->second(this);
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

// parses the command and returns the target row (or -1/-2 based on the command)
int Executor::parse_command(const string &command) {
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
        // if the operator is empty, ignore
        if (op.empty()) {
            return -1;
        }

        // operator is available in special operator map
        // it should be sent to all ranks
        if (Executor::special_op_map.find(op) != Executor::special_op_map.end()) {
            return -2;
        }

        // otherwise just show error message
        return -3;
    }

    return row;
}

string Executor::get_row(Executor *executor, int row) {
    stringstream res_str;

    int local_row = executor->get_local_row(row);
    if (local_row < 0) {
        return "error: invalid row value (out of range).";
    }

    res_str << "row " << row << " (" << executor->rank << ":" << local_row << "): ";

    string sep = "{ ";
    for (const auto &r: executor->array_part[local_row]) {
        res_str << sep << r;
        sep = ", ";
    }

    res_str << " }";

    return res_str.str();
}

string Executor::get_aggr(Executor *executor, int row) {
    stringstream res_str;

    int local_row = executor->get_local_row(row);
    if (local_row < 0) {
        return "error: invalid row value (out of range).";
    }

    int aggr = 0;
    for (const auto &r: executor->array_part[local_row]) {
        aggr += r;
    }

    res_str << "aggr " << row << " (" << executor->rank << ":" << local_row << "): " << aggr;

    return res_str.str();
}

string Executor::exit(Executor *executor) {
    return "exited";
}

map<string, string (*)(Executor *)> Executor::special_op_map = {
        {"exit", exit}
};
