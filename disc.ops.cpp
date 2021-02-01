#include <stdio.h>
#include <fcntl.h> 
#include <errno.h>
#include <unistd.h>
#include <variant>
#include <string>
#include "query_parser.cpp"
using namespace std;

#define SCHEMA_ATTRIBUTE_SIZE 16 // each schema attribute like name, roll etc will acquire this much size in schema file
#define SCHEMA_TYPE_SIZE 8 // each schema attribute type like int, string etc will acquire this much size in schema file

#define META_TUPLE_ADDRESS 4 // The address at which new tuple will be inserted. Takes 4 bytes in meta file.
#define META_DATABASE_SIZE 4 // Size of database. Takes 4 bytes in meta file
#define META_NUM_TUPLES 4 // Number of tuples in database. Take 4 bytes in meta file
#define META_TUPLE_SIZE 4 // Size of each tuple. Take 4 bytes in meta file

extern int errno; // for printing errors

// takes a name as input and makes a file
// used to create to .schema .meta and .db files when first initializing a database
int create_file(char* name){
    int fd = creat(name, 0777);
    if(fd == -1){
        cout << "unable to create" << endl;
        close(fd);
        return -1;
    }
    else{
        close(fd);
        return 1;
    }
}

/**
 * After creating .meta file, it is initialized
 * meta file contains five information as of now
 * Last tuple address = address to insert new row
 * Database size
 * Number of tuples in database
 * Tuple size
 * Database name --> which takes 32 bytes in meta file
 * parameters:
 * char* name = .meta file
 * dbname = name of database
 **/
int Initialize_Meta_File(char* name, int tupleSize, string dbname){
    int fd = open(name, O_WRONLY);
    int offset = 0, nor = 0;

    lseek(fd, offset, SEEK_SET);
    ssize_t bytes = write(fd, (void*)&nor, META_TUPLE_ADDRESS);
    offset += META_TUPLE_ADDRESS;

    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)&nor, META_DATABASE_SIZE);
    offset += META_DATABASE_SIZE;

    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)&nor, META_NUM_TUPLES);
    offset += META_NUM_TUPLES;

    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)&tupleSize, META_TUPLE_SIZE);
    offset += META_TUPLE_SIZE;

    char* temp = (char*) malloc(STRING * sizeof(char));
    strcpy(temp, dbname.c_str());

    lseek(fd, offset, SEEK_SET);
    write(fd, (void*)temp, STRING);
    offset += STRING;

    close(fd);
    free(temp);
    return 1;
}

/**
 * Read the address at which new tuple will be inserted in data file
 * char* name = .meta file
 **/
int Read_Last_Tuple_Address(char* name){
    int fd = open(name, O_RDONLY);
    void* buffer = malloc(4);
    ssize_t bytes = read(fd, buffer, META_TUPLE_ADDRESS);
    int* p = (int*)buffer;
    close(fd);
    return *p;
}

/**
 * Updates the address at which new tuple will be inserted in data file
 * char* name = .meta file
 * address = address at which tuple will be inserted
 **/
int Update_Last_Tuple_Address(char* name, int address){
    int fd = open(name, O_WRONLY);
    ssize_t bytes = write(fd, (void*)&address, META_TUPLE_ADDRESS);
    close(fd);
    return 1;
}

/**
 * Read database size
 * char* name = .meta file
 **/
int Read_Database_Size(char* name){
    int fd = open(name, O_RDONLY);
    void* buffer = malloc(4);
    lseek(fd, 4, SEEK_SET);
    ssize_t bytes = read(fd, buffer, META_TUPLE_ADDRESS);
    int* p = (int*)buffer;
    close(fd);
    return *p;
}

/**
 * Updates database size
 * char* name = .meta file
 **/
int Update_Database_Size(char* name, int newsize){
    int fd = open(name, O_WRONLY);
    lseek(fd, 4, SEEK_SET);
    ssize_t bytes = write(fd, (void*)&newsize, META_DATABASE_SIZE);
    close(fd);
    return 1;
}

/**
 * reads number of tuples in database
 * char* name = .meta file
 **/
int Read_Num_Tuples(char* name){
    int fd = open(name, O_RDONLY);
    void* buffer = malloc(4);
    lseek(fd, 8, SEEK_SET);
    ssize_t bytes = read(fd, buffer, META_TUPLE_ADDRESS);
    int* p = (int*)buffer;
    close(fd);
    return *p;
}

/**
 * updates number of tuples in database
 * char* name = .meta file
 **/
int Update_Num_Tuples(char* name, int tuples){
    int fd = open(name, O_WRONLY);
    lseek(fd, 8, SEEK_SET);
    ssize_t bytes = write(fd, (void*)&tuples, META_NUM_TUPLES);
    close(fd);
    return 1;
}

/**
 * Reads size of each tuple
 * char* name = .meta file
 **/
int Read_Tuple_Size(char* name){
    int fd = open(name, O_RDONLY);
    void* buffer = malloc(4);
    lseek(fd, 12, SEEK_SET);
    ssize_t bytes = read(fd, buffer, META_TUPLE_ADDRESS);
    int* p = (int*)buffer;
    close(fd);
    return *p;
}

string Read_DatabaseName(char* name){
    int fd = open(name, O_RDONLY);
    char* buffer = (char*) malloc(STRING * sizeof(char));
    lseek(fd, 16, SEEK_SET);
    ssize_t bytes = read(fd, (void*)buffer, STRING);
    return buffer;
}

/**
 * Schema file contains contains schema details in pair
 * First = name of attribute
 * Second = Data type of attribute
 * schema = contains schema info in pairs like (name, string), (roll, int) etc
 * name = .schema file
 **/
int Write_In_Schema_File(char* name, vector<pair<string,string>> schema){
    int fd = open(name, O_WRONLY);
    char* attr = (char*) malloc(SCHEMA_ATTRIBUTE_SIZE * sizeof(char));
    char* type = (char*) malloc(SCHEMA_TYPE_SIZE * sizeof(char));
    int offset = 0;
    for(pair<string,string> p: schema){
        strcpy(attr, p.first.c_str());
        strcpy(type, p.second.c_str());
        lseek(fd, offset, SEEK_SET);
        ssize_t written1 = write(fd, (void*)attr, SCHEMA_ATTRIBUTE_SIZE);
        offset += SCHEMA_ATTRIBUTE_SIZE;
        lseek(fd, offset, SEEK_SET);
        ssize_t written2 = write(fd, (void*)type, SCHEMA_TYPE_SIZE);
        offset += SCHEMA_TYPE_SIZE;
    }
    close(fd);
    free(attr);
    free(type);
    return 1;
}

/**
 * reads the schema from schema file and returns array of pairs.
 * Example (name, string) (roll, int) etc
 * char* name = .schema file
 **/
vector<pair<string,string>> Read_Schema_File(char* name){
    vector<pair<string,string>> schema;
    int fd = open(name, O_RDONLY);
    char* attr = (char*) malloc(SCHEMA_ATTRIBUTE_SIZE * sizeof(char));
    char* type = (char*) malloc(SCHEMA_TYPE_SIZE * sizeof(char));
    int offset = 0;
    while(1){
        lseek(fd, offset, SEEK_SET);
        ssize_t bytes = read(fd, (void*)attr, SCHEMA_ATTRIBUTE_SIZE);
        if(bytes == 0) break;
        offset += SCHEMA_ATTRIBUTE_SIZE;
        lseek(fd, offset, SEEK_SET);
        ssize_t bytes1 = read(fd, (void*)type, SCHEMA_TYPE_SIZE);
        if(bytes1 == 0) break;
        offset += SCHEMA_TYPE_SIZE;
        schema.push_back({ attr, type });
    }
    free(attr);
    free(type);
    close(fd);
    return schema;
}

/**
 * writes data in .db file and updates .meta file
 * tuple = array containing tuple info in the same order as in schema
 * char* name = .db file
 * char* meta = .meta file
 * tuplesize = size of each tuple in this database.
 **/
int Write_In_Data_File(char* name, char* meta, vector<AttributeNode*> tuple, int tuplesize){
    int last_address = Read_Last_Tuple_Address(meta);
    int fd = open(name, O_WRONLY), offset = last_address;
    for(AttributeNode* node: tuple){
        lseek(fd, offset, SEEK_SET);
        ssize_t bytes;
        if(node->index == 0){
            bytes = write(fd, (void*)&node->num, INTEGER);
            offset += INTEGER;
        }
        else if(node->index == 1){
            bytes = write(fd, (void*)&node->b, BOOL);
            offset += BOOL;
        }
        else{
            bytes = write(fd, (void*)node->str, STRING);
            offset += STRING;
        }
    }
    // free(temp);
    close(fd);
    Update_Last_Tuple_Address(meta, last_address + tuplesize);
    Update_Database_Size(meta, Read_Database_Size(meta) + tuplesize);
    Update_Num_Tuples(meta, Read_Num_Tuples(meta) + 1);
    return 1;
}

/**
 * This functions writes data at a specified index like writing it at index 2 or 3 etc.
 * This is used in deletion and updation.
 * When a tuple is deleted, the last tuple is copied to the deleted tuple position or index and .meta file is updated.
 * tuple = same as above
 * tupleNum = index to write tuple
 * tuplesize = size of each tuple
 * char* name = .db file
 **/
int Write_At_Location(char* name, vector<AttributeNode*> tuple, int tupleNum, int tupleSize){
    int fd = open(name, O_WRONLY);
    int offset = tupleNum * tupleSize;
    for(AttributeNode* node: tuple){
        lseek(fd, offset, SEEK_SET);
        ssize_t bytes;
        if(node->index == 0){
            bytes = write(fd, (void*)&node->num, INTEGER);
            offset += INTEGER;
        }
        else if(node->index == 1){
            bytes = write(fd, (void*)&node->b, BOOL);
            offset += BOOL;
        }
        else{
            bytes = write(fd, (void*)node->str, STRING);
            offset += STRING;
        }
    }
    return 1;
}

/**
 * Reads the tuple at the specified position (tupleNum)
 * schema = schema in pair form like (name, string), (roll, int) etc
 * tupleSize = size of each tuple
 * tupleNum = positon or index of the tuple to read
 * char* name = .db file
 **/
vector<AttributeNode*> Read_Data_File(char* name, vector<pair<string,string>> schema, int tupleNum, int tuplesize){
    vector<AttributeNode*> tuple((int)schema.size());
    int index = 0, offset = tupleNum * tuplesize;
    int fd = open(name, O_RDONLY);
    void* num = malloc(INTEGER), *b = malloc(BOOL);
    char* temp = (char*) malloc(STRING * sizeof(char));
    for(pair<string,string> sch: schema){
        lseek(fd, offset, SEEK_SET);
        if(sch.second == TYPE1){
            read(fd, num, INTEGER);
            int* p = (int*)num;
            AttributeNode* node = getAttributeNode(*p, false, "", 0);
            tuple[index++] = node;
            offset += INTEGER;
        }
        else if(sch.second == TYPE2){
            read(fd, (void*)temp, STRING);
            tuple[index++] = getAttributeNode(1, false, temp, 2);
            offset += STRING;
        }
        else{
            read(fd, b, BOOL);
            bool* p = (bool*)b;
            tuple[index++] = getAttributeNode(1, *p, "", 1);
            offset += BOOL;
        }
    }
    close(fd);
    free(num);
    free(b);
    free(temp);
    return tuple;
}

/**
 * Deleted tuple at the specified position or index (tupleNum)
 * copies the last tuple to the deleted position or index
 * .meta file is updated
 * char* meta = .meta file
 * char* name = .db file
 * tupleNum = position or index of tuple to be deleted
 * tupleSize = size of each tuple
 **/
int Delete_Data_Tuple(char* name, char* meta, vector<pair<string,string>> schema, int tupleNum, int tupleSize){
    vector<AttributeNode*> tuple = Read_Data_File(name, schema, Read_Num_Tuples(meta) - 1, tupleSize);
    Write_At_Location(name, tuple, tupleNum, tupleSize);
    Update_Last_Tuple_Address(meta, Read_Last_Tuple_Address(meta) - tupleSize);
    Update_Database_Size(meta, Read_Database_Size(meta) - tupleSize);
    Update_Num_Tuples(meta, Read_Num_Tuples(meta) - 1);
    return 1;
}

int main(){
    /**
     * schema creation syntax
     * 1 create schema database_name(attr type, attr type, ......)
     * 
     * insert syntax
     * 2 database_name attr1value attr2value attr3value ........
     * 
     * print all tuples syntax
     * 3 database_name
     * 
     * print meta and schema file contents syntax
     * 4 database_name
     * 
     * Delete syntax
     * 5 database_name index_of_the_tuple (0 based index)
     **/
    while(1){
        cout << "enter query: ";
        string query;
        getline(cin, query);
        if(query[0] == '1'){
            int sp = query.find(' ');
            query = query.substr(sp + 1, query.size() - sp);
            vector<pair<string,string>> schema = parse_schema_DDL(query);
            string dbname = getDatabaseName(query);
            char* name = (char*) malloc(STRING * sizeof(char));
            strcpy(name, (dbname + ".schema").c_str());
            create_file(name);
            cout << "created schema file" << endl;
            Write_In_Schema_File(name, schema);
            int tupleSize = getTupleSize(schema);
            cout << "wrote schema in schema file" << endl;
            strcpy(name, (dbname + ".meta").c_str());
            create_file(name);
            cout << "created metadata file" << endl;
            Initialize_Meta_File(name, tupleSize, dbname);
            cout << "initialized metadata file" << endl;
            tupleSize = Read_Tuple_Size(name);
            cout << "tupleSize: " << tupleSize << endl;
            strcpy(name, (dbname + ".db").c_str());
            create_file(name);
            cout << "created database file" << endl;
            strcpy(name, (dbname + ".avl").c_str());
            create_file(name);
            cout << "created avl index file" << endl;
            strcpy(name, (dbname + ".btree").c_str());
            create_file(name);
            cout << "created B-Tree index file" << endl;
            free(name);
        }
        else if(query[0] == '2'){
            int sp = query.find(' ') + 1;
            string dbname = query.substr(sp, query.find(' ', sp) - sp);
            char* name = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            strcpy(name, (dbname + ".schema").c_str());
            vector<AttributeNode*> tuple = getTuple(Read_Schema_File(name), query.substr(query.find(' ', sp) + 1, query.size() - query.find(' ', sp)));
            int tupleSize = getTupleSize(Read_Schema_File(name));
            strcpy(name, (dbname + ".db").c_str());
            strcpy(meta, (dbname + ".meta").c_str());
            Write_In_Data_File(name, meta, tuple, tupleSize);
            free(name);
            free(meta);
        }
        else if(query[0] == '3'){
            char* name = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            string dbname = query.substr(query.find(' ') + 1, query.size() - query.find(' '));
            strcpy(meta, (dbname + ".meta").c_str());
            strcpy(name, (dbname + ".db").c_str());
            int size = Read_Num_Tuples(meta);
            strcpy(meta, (dbname + ".schema").c_str());
            vector<pair<string,string>> schema = Read_Schema_File(meta);
            cout << "Tuple: " << endl;
            for(int i = 0; i < size; i++){
                vector<AttributeNode*> tuple = Read_Data_File(name, schema, i, getTupleSize(schema));
                for(AttributeNode* node: tuple){
                    if(node->index == 0) cout << node->num << " ";
                    else if(node->index == 1) cout << node->b << " ";
                    else cout << node->str << " ";
                }
                cout << endl;
            }
            free(name);
            free(meta);
        }
        else if(query[0] == '4'){
            string dbname = query.substr(query.find(' ') + 1, query.size() - query.find(' '));
            char* meta = (char*) malloc(STRING * sizeof(char));
            strcpy(meta, (dbname + ".meta").c_str());
            cout << "Last tuple address: " << Read_Last_Tuple_Address(meta) << endl;
            cout << "Database size: " << Read_Database_Size(meta) << endl;
            cout << "Number of Tuples: " << Read_Num_Tuples(meta) << endl;
            cout << "Tuple Size: " << Read_Tuple_Size(meta) << endl;
            cout << "Database name: " << Read_DatabaseName(meta) << endl;
            strcpy(meta, (dbname + ".schema").c_str());
            cout << "Schema: " << endl;
            vector<pair<string,string>> schema = Read_Schema_File(meta);
            for(pair<string,string> p: schema){
                cout << p.first << " " << p.second << endl;
            }
        }
        else if(query[0] == '5'){
            int fsp = query.find(' '), ssp = query.find(' ', fsp + 1);
            string dbname = query.substr(fsp + 1, ssp - fsp - 1);
            int tupleNum = stoi(query.substr(ssp + 1, query.size() - ssp));
            // cout << dbname << " " << tupleNum << endl;
            char* name = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            strcpy(name, (dbname + ".schema").c_str());
            strcpy(meta, (dbname + ".meta").c_str());
            vector<pair<string,string>> schema = Read_Schema_File(name);
            strcpy(name, (dbname + ".db").c_str());
            Delete_Data_Tuple(name, meta, schema, tupleNum, Read_Tuple_Size(meta));
        }
    }
}
