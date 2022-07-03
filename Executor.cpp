//
// Created by Writwick Das on 21/06/2022.
//

#include <sstream>
#include <algorithm>
#include <cmath>

#include "Executor.h"

using namespace std;

// static variable to store return values that are not directly allocated into the array
static vector<int> const_result;

// executes the command in local context and sends back the result
int *Executor::execute_command(string command, int &count) {
    // convert command to lowercase first
    transform(command.begin(), command.end(), command.begin(),
              [](unsigned char c) { return tolower(c); });

    stringstream res_str;
    istringstream string_stream(command);

    string op, sub_op, row_str, row_end_str;

    // split the command into op, sub op and row index strings
    getline(string_stream, op, ' ');
    getline(string_stream, sub_op, ' ');
    getline(string_stream, row_str, '-');
    getline(string_stream, row_end_str, ' ');

    // try to parse the row value into integer
    stringstream row_stream(row_str);
    int row(0);
    row_stream >> row;

    // try to parse the row end value into integer
    stringstream row_end_stream(row_end_str);
    int row_end(0);
    row_end_stream >> row_end;

    // try to find operator in special operator map keys
    auto sp_op_element = Executor::special_op_map.find(op);
    if (sp_op_element != Executor::special_op_map.end()) {
        // it's a valid special operator so return its result directly
        return sp_op_element->second(this, count);
    }

    // try to find operator in operator map keys and range operator map keys
    auto op_element = op_map.find(op);
    auto range_op_element = range_op_map.find(op);

    if (op_element == op_map.end() && range_op_element == range_op_map.end()) {
        // operator is not valid
        res_str << "error: operator \"" << op << "\" is invalid.";
        cout << "rank " << this->rank << " >> " << res_str.str() << endl;

        const_result = vector<int>{-1};

        count = const_result.size();
        return const_result.data();
    }

    // we disregard if the range parse was successful or not so that
    // we don't call a normal function when there is an invalid row end value
    // and this is another way to check if there was any value passed as a
    // possible row end value
    if (row_end_str.empty()) {
        // not a range operator call
        const auto sub_op_map = op_element->second;

        auto sub_op_element = sub_op_map.find(sub_op);
        if (sub_op_element == sub_op_map.end()) {
            // sub-operator is not valid for operator
            res_str << "error: sub operator \"" << sub_op << "\" is invalid for operator \"" << op << "\".";
            cout << "rank " << this->rank << " >> " << res_str.str() << endl;

            const_result = vector<int>{-1};

            count = const_result.size();
            return const_result.data();
        }
        const auto op_func = sub_op_element->second;

        if (row_stream.fail()) {
            res_str << "error: failed to parse row index.";
            cout << "rank " << this->rank << " >> " << res_str.str() << endl;

            const_result = vector<int>{-1};

            count = const_result.size();
            return const_result.data();
        }

        // call the op_func to execute the command
        return op_func(this, row, count);
    } else {
        const auto range_sub_op_map = range_op_element->second;

        auto range_sub_op_element = range_sub_op_map.find(sub_op);
        if (range_sub_op_element == range_sub_op_map.end()) {
            // sub-operator is not valid for operator
            res_str << "error: sub operator \"" << sub_op << "\" is invalid for range operator \"" << op << "\".";
            cout << "rank " << this->rank << " >> " << res_str.str() << endl;

            const_result = vector<int>{-1};

            count = const_result.size();
            return const_result.data();
        }
        const auto range_op_func = range_sub_op_element->second;

        if (row_stream.fail()) {
            res_str << "error: failed to parse row index.";
            cout << "rank " << this->rank << " >> " << res_str.str() << endl;

            const_result = vector<int>{-1};

            count = const_result.size();
            return const_result.data();
        }

        if (row_end_stream.fail()) {
            res_str << "error: failed to parse row end index.";
            cout << "rank " << this->rank << " >> " << res_str.str() << endl;

            const_result = vector<int>{-1};

            count = const_result.size();
            return const_result.data();
        }

        // call the range_op_func to execute the command
        return range_op_func(this, row, row_end, count);
    }
}

// parses the command and returns the parsing result
// returns a list of sub commands to send to other ranks in sub_command_map
P_RESULT Executor::parse_command(const string &command, map<int, pair<int, int>> &sub_command_map,
                                 string &command_prefix) const {
    istringstream string_stream(command);
    stringstream prefix_stream;

    string op, sub_op, row_str, row_end_str;

    // split the command into op, sub op and row index strings
    getline(string_stream, op, ' ');
    getline(string_stream, sub_op, ' ');
    getline(string_stream, row_str, '-');
    getline(string_stream, row_end_str, ' ');

    prefix_stream << op << " " << sub_op << " ";
    command_prefix = prefix_stream.str();

    if (row_str.substr(0, 3) == "all") {
        row_str = "0";
        row_end_str = to_string(this->N);
    }

    // try to parse the row value into integer
    stringstream row_stream(row_str);
    int row(-3);
    row_stream >> row;

    // try to parse the row end value into integer
    stringstream row_end_stream(row_end_str);
    int row_end(-3);
    row_end_stream >> row_end;
    sub_command_map.clear();

    if (row_stream.fail()) {
        // if the operator is empty, ignore
        if (op.empty()) {
            return EMPTY_OP;
        }

        // operator is available in special operator map
        // it should be sent to all ranks
        if (Executor::special_op_map.find(op) != Executor::special_op_map.end()) {
            return SPECIAL_OPERATOR;
        }

        // otherwise just show error message
        return ERROR_OPERATOR;
    }

    if (row > this->N || row < 0) {
        // row out of range
        sub_command_map.insert({0, {row / this->N1, row}});
    }

    if (row_end_str.length() > 0) {
        // it is a range command
        if (row_end_stream.fail()) {
            // failed to parse row end
            return ERROR_OPERATOR;
        }

        if (row_end > this->N || row_end < 0) {
            // end row out of range
            sub_command_map.insert({-1, {row / this->N1, row}});
            sub_command_map.insert({-2, {row_end / this->N1, row_end}});
        }
    }

    if (!sub_command_map.empty()) {
        return ROW_OUT_OF_RANGE;
    }

    if (row > row_end) {
        sub_command_map.insert({-1, {row, row_end}});
        return NEGATIVE_ROW_RANGE;
    }

    for (int rnk = row / this->N1;
         row_end % this->N1 == 0 ? rnk < row_end / this->N1 : rnk <= row_end / this->N1; rnk++) {
        int rnk_row_start = max(0, row - rnk * this->N1), rnk_row_end = min(row_end - rnk * this->N1, this->N1);

        sub_command_map.insert({rnk, {rnk_row_start, rnk_row_end}});
    }

    return SUCCESS;
}

int *Executor::get_row(Executor *executor, int row, int &count) {
    stringstream res_str;

    count = executor->array_part[row].size();
    return executor->array_part[row].data();
}

int *Executor::get_aggr_range(Executor *executor, int row_start, int row_end, int &count) {
    stringstream res_str;

    int aggr = 0;
    for (int row = row_start; row < row_end; ++row) {
        for (const auto &r: executor->array_part[row]) {
            aggr += r;
        }
    }

    const_result = vector<int>{aggr};

    count = const_result.size();
    return const_result.data();
}


int *Executor::get_aggr(Executor *executor, int row, int &count) {
    stringstream res_str;

    int aggr = 0;
    for (const auto &r: executor->array_part[row]) {
        aggr += r;
    }

    const_result = vector<int>{aggr};

    count = const_result.size();
    return const_result.data();
}

int *Executor::exit(Executor *executor, int &count) {
    count = 1;
    cout << "rank " << executor->rank << " >> exited" << endl;
    const_result = vector<int>{-1};

    count = const_result.size();
    return const_result.data();
}

map<string, int *(*)(Executor *, int &)> Executor::special_op_map = {
        {"exit", exit}
};
