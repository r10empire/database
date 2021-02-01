#include <iostream>
#include <string.h>
#include <vector>
using namespace std;

#define CREATE_SCHEMA 1 // not used

#define INTEGER 4 // Integer attribute data will take this much size in .db file
#define BOOL 1 // Bool attribute data will take this much size in .db file
#define STRING 32 // String attribute data will take this much size in .db file

// The three datatypes we decided to use
const string TYPE1 = "int";
const string TYPE2 = "string";
const string TYPE3 = "bool";

// Every attribute in RAM will have this form
// when it is a integer attribute, index = 0
// when bool, index = 1
// when string, index = 2
struct AttributeNode{
    int num;
    bool b;
    char* str;
    int index;
};

// this will return a AttributeNode*
// index = 0 when int
// index = 1 when bool
// index = 2 when string
AttributeNode* getAttributeNode(int num, bool b, string str, int index){
    AttributeNode* node = (AttributeNode*) malloc(sizeof(AttributeNode));
    node->str = (char*) malloc(STRING * sizeof(char));
    node->index = index;
    if(index == 0) node->num = num;
    else if(index == 1) node->b = b;
    else{
        strcpy(node->str, str.c_str());
    }
    return node;
}

// returns tuple size taking schema as input
int getTupleSize(vector<pair<string,string>> schema){
    int tot = 0;
    for(pair<string,string> p: schema){
        if(p.second == TYPE1) tot += INTEGER;
        else if(p.second == TYPE2) tot += STRING;
        else tot += BOOL;
    }
    return tot;
}

/**
 * parses a insert query and returns array of AttributeNodes
 * query is of following type:
 * rahul 24 male
 **/
vector<AttributeNode*> getTuple(vector<pair<string,string>> schema, string query){
    vector<AttributeNode*> tuple(schema.size());
    int index = 0, i = 0;
    string str = "";
    query += " ";
    while(i < query.size()){
        if(query[i] == ' ' && str != ""){
            if(schema[index].second == TYPE1){
                tuple[index++] = getAttributeNode(stoi(str), false, "", 0);
            }
            else if(schema[index].second == TYPE2){
                tuple[index++] = getAttributeNode(1, false, str, 2);                
            }
            else
                tuple[index++] = getAttributeNode(1, stoi(str), "", 1);
            str = "";
        }
        else str += query[i];
        i++;
    }
    return tuple;
}

string getDatabaseName(string query){
    int sp = query.find("schema") + 7,
    obrace = query.find("(");
    return query.substr(sp, obrace - sp);
}

/**
 * takes create schema (DDL) query and returns schema in pairs
 * query is of following form
 * create schema test(name string, roll int, engineer bool)
 **/
vector<pair<string,string>> parse_schema_DDL(string query){
    vector<pair<string,string>> schema;
    int obrace = query.find('('), cbrace = query.find(')');
    string attr = "", type = "";
    int i = obrace + 1, index = 0;
    while(i < cbrace){
        if(query[i] == ' '){
            if(attr.size() > 0){
                schema.push_back({ attr, "" });
                attr = "";
            }
            i++;
            continue;
        }
        else if(query[i] == ','){
            if(attr.size() > 0){
                schema[index].second = attr;
                index++;
                attr = "";
            }
            i++;
            continue;
        }
        attr += query[i];
        i++;
    }
    if(attr.size() > 0) schema[index].second = attr;
    return schema;
}

// int main(){
//     string query;
//     getline(cin, query);
//     vector<pair<string,string>> arr = parse_schema_DDL(query);
//     for(pair<string,string> p: arr) cout << p.first << " " << p.second << endl;
// }
