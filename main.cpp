#include <iostream>
#include <string>
#include <cstring>
#include <sstream>
#include <vector>
#include <sys/types.h>
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
using namespace std;

enum ExecuteResult {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
};

enum MetaCommandResult{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};

enum PrepareResult{
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
};

enum StatementType{
    STATEMENT_INSERT, 
    STATEMENT_SELECT
};

class InputBuffer {
    public:
        string buffer;
        size_t buffer_length;
        ssize_t input_length;
        InputBuffer() : buffer(""), buffer_length(0), input_length(0) {}
};

class Row {
    public:
        uint32_t id;
        string username;
        string email;
        // Row() : id(0), username(""), email("") {}
};

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

class Statement {
    public:
        StatementType type;
        Row row_to_insert;
};

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

class Table {
    public:
        uint32_t num_rows;
        void* pages[TABLE_MAX_PAGES];
        Table() {
            this->num_rows = 0;
            for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
                this->pages[i] = NULL;
            }
        }
};

void free_table(Table table) {
    delete &table;
}

void serialize_row(Row source, void* destination) {
    memcpy(&destination + ID_OFFSET, &source.id, ID_SIZE);
    memcpy(&destination + USERNAME_OFFSET, &source.username, USERNAME_SIZE);
    memcpy(&destination + EMAIL_OFFSET, &source.email, EMAIL_SIZE);
}

void deserialize_row(void* source, Row destination) {
    memcpy(&destination.id, &source + ID_OFFSET, ID_SIZE);
    memcpy(&destination.username, &source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&destination.email, &source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* row_slot(Table table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = table.pages[page_num];
    if(page == NULL) 
        page = table.pages[page_num] = malloc(PAGE_SIZE);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset % ROW_SIZE;
    return &page + byte_offset;
}

void print_row(Row row) {
    cout << row.id << ", " << row.username << ", " << row.email << endl;
}

void print_prompt() {
    cout << "db > ";
}

string read_input(InputBuffer input_buffer) {
    getline(cin, input_buffer.buffer);
    ssize_t bytes_read = input_buffer.buffer.size();
    if(bytes_read <= 0) {
        cout << "Error reading input!" << endl;
        exit(EXIT_FAILURE);
    }

    input_buffer.input_length = bytes_read;
    input_buffer.buffer[bytes_read] = 0;
    return input_buffer.buffer;
}

void close_input_buffer(InputBuffer input_buffer) {
    delete &input_buffer;
}

MetaCommandResult do_meta_command(InputBuffer input_buffer, string ip_buffer, Table table) {
    if(ip_buffer.compare(".exit") == 0) {
        close_input_buffer(input_buffer);
        free_table(table);
        exit(EXIT_SUCCESS);
    }
    else 
        return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_statement(InputBuffer input_buffer, string ip_buffer, Statement& statement) {
    if(ip_buffer.substr(0, 6) == "insert") {
        statement.type = STATEMENT_INSERT;
        vector<string> input_string;
        stringstream ss(ip_buffer);
        string temp;
        while(getline(ss, temp, ' ')) {
            input_string.push_back(temp);
        }
        if(input_string.size() < 3) {
            return PREPARE_SYNTAX_ERROR;
        }
        try {
            statement.row_to_insert.id = stoi(input_string[1]);
        } 
        catch(const std::exception& e) {
            return PREPARE_SYNTAX_ERROR;
        }
        statement.row_to_insert.username = input_string[2];
        statement.row_to_insert.email = input_string[3];
        return PREPARE_SUCCESS;
    }
    if(ip_buffer.compare("select") == 0) {
        statement.type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement statement, Table& table) {
    if(table.num_rows >= TABLE_MAX_ROWS)
        return EXECUTE_TABLE_FULL;
    Row row_to_insert = statement.row_to_insert;
    serialize_row(row_to_insert, row_slot(table, table.num_rows));
    table.num_rows += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement statement, Table table) {
    Row row;
    for(uint32_t i = 0; i < table.num_rows; i++) {
        deserialize_row(row_slot(table, i), row);
        print_row(row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement statement, Table& table) {
    switch(statement.type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

int main() {
    Table table;
    InputBuffer input_buffer;
    while(1) {
        print_prompt();
        string ip_buffer = read_input(input_buffer);
        if(ip_buffer[0] == '.') {
            switch(do_meta_command(input_buffer, ip_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    cout << "Unrecognized command '" << ip_buffer << "'" << endl;
                    continue;
            }
        }

        Statement statement;
        switch(prepare_statement(input_buffer, ip_buffer, statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                cout << "Syntax error. Could not parse statement." << endl;
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                cout << "Unrecognized keyword at start of '" << ip_buffer << "'." << endl;
                continue;
        }
        switch(execute_statement(statement, table)) {
            case (EXECUTE_SUCCESS):
                cout << "Executed." << endl;
                break;
            case (EXECUTE_TABLE_FULL):
                cout << "Error: Table Full." << endl;
                break;
        }
    }
}