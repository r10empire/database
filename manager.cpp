#include "avl_ops.cpp"
#include <chrono>
using namespace std::chrono;

#define SPACE 20
#define LINEHEIGHT 3

vector<int> index_attr; // assuming it is sorted
vector<char*> names;

void InitializeIndexes(){
    for(int i = 0; i < (int)index_attr.size(); i++){
        create_file(names[i]);
        Initialize_AVLindex(names[i]);
    }
}

void Initialize_DB(char* name, char* meta){
    if((int)index_attr.size() > 0) return;
    index_attr = Read_Index(meta);
    for(int i = 0; i < (int)index_attr.size(); i++){
        string curr = name;
        curr += to_string(index_attr[i]);
        curr += ".avl";
        names.push_back((char*) malloc(STRING * sizeof(char)));
        strcpy(names[i], curr.c_str());
    }
}

int Update_Index(char* name, char* meta, vector<AttributeNode*> tuple, int blockaddr){
    if((int)names.size() == 0){
        Initialize_DB(name, meta);
    }
    for(int i = 0; i < (int)names.size(); i++){
        int key;
        if(tuple[index_attr[i]]->index == 0){
            key = tuple[index_attr[i]]->num;
        }
        else{
            continue;
        }
        int root = Read_4Bytes_Address(names[i], 4);
        root = Insert_INT_AVLindex(names[i], root, -1, key, blockaddr, index_attr[i]);
        Update_Root_Address(names[i], root);
    }
    return 1;
}

void PrintTuple(vector<AttributeNode*> tuple){
    for(AttributeNode* node: tuple){
        int len = 0;
        if(node->index == 0){
            cout << node->num << " ";
            len += (int)to_string(node->num).size();
        }
        else if(node->index == 1){
            cout << node->b << " ";
            len++;
        }
        else{
            cout << node->str << " ";
            len += (int)strlen(node->str);
        }
        len = (int)SPACE - len - 1;
        while(len > 0){
            cout << " ";
            len--;
        }
        cout << "|";
    }
    cout << endl;
}

void PrintSchema(vector<pair<string,string>> schema){
    for(pair<string,string> p: schema){
        cout << p.first << " ";
        int len = (int)p.first.size();
        len = SPACE - len - 1;
        while(len > 0){
            cout << " ";
            len--;
        }
        cout << "|";
    }
    cout << endl;
}

void PrintLines(int times){
    while(times > 0){
        cout << endl;
        times--;
    }
}

void PrintSpace(int times){
    while(times > 0){
        cout << " ";
        times--;
    }
}

void PrintUnderScore(int times){
    while(times > 0){
        cout << "_";
        times--;
    }
    cout << endl;
}

void PrintSchemaWithType(vector<pair<string,string>> schema){
    PrintUnderScore(2 * (int)SPACE);
    cout << "Attribute Name";
    PrintSpace(5);
    cout << "|";
    cout << "Attribute Type";
    PrintSpace(5);
    cout << "|";
    cout << endl;
    PrintUnderScore(2 * (int)SPACE);
    for(pair<string,string> p: schema){
        cout << p.first;
        PrintSpace(SPACE - p.first.size() - 1);
        cout << "|";
        cout << p.second;
        PrintSpace(SPACE - p.first.size() - 1);
        cout << "|" << endl;
        PrintUnderScore(2 * (int)SPACE);
    }
}

vector<int> Search_Index(int i, int val){
    vector<int> arr;
    int root = Read_4Bytes_Address(names[i], 4);
    int avlNodeLoc = Search_INT_AVLindex(names[i], root, -1, val, index_attr[i]);
    if(avlNodeLoc != -1){
        AVLNODE* node = Read_AVLNODE(names[i], avlNodeLoc, 0);
        for(auto i: node->blocks){
            arr.push_back(i);
        }
    }
    return arr;
}

int BinarySearch(int val){
    int l = 0, u = (int)index_attr.size() - 1;
    while(l <= u){
        int mid = (l + u) / 2;
        if(index_attr[mid] == val) return mid;
        else if(val < index_attr[mid]) u = mid - 1;
        else l = mid + 1;
    }
    return -1;
}

// vector<int> Search_EntireDB(char* dbname, )

void FreeTuple(vector<AttributeNode*> tuple){
    for(AttributeNode* node: tuple){
        free(node->str);
        free(node);
    }
}

vector<int> GetPositions(vector<pair<string,string>> schema, vector<pair<string,string>> arr){
    vector<int> positions((int)arr.size());
    for(int i = 0; i < (int)arr.size(); i++){
        for(int j = 0; j < (int)schema.size(); j++){
            if(schema[j].first == arr[i].first){
                positions[i] = j;
                break;
            }
        }
    }
    return positions;
}

int DropDatabase(char* name, char* dbname, char* meta, char* schname){
    Initialize_DB(name, meta);
    remove(dbname);
    remove(meta);
    remove(schname);
    string str = name;
    strcpy(name, (str + ".avl").c_str());
    remove(name);
    strcpy(name, (str + ".btree").c_str());
    remove(name);
    for(char* name: names){
        remove(name);
        free(name);
    }
    index_attr.clear();
    names.clear();
    return 1;
}

vector<int> Search_Database(char* name, char* dbname, char* meta, vector<pair<string,string>> schema, vector<pair<string,string>> arr){
    vector<int> positions = GetPositions(schema, arr);
    Initialize_DB(name, meta);
    
    for(int i = 0; i < (int)arr.size(); i++){
        int bs = BinarySearch(positions[i]);
        if(bs != -1){
            vector<int> blocks = Search_Index(bs, stoi(arr[i].second));
            vector<int> res;
            for(int i = 0; i < (int)blocks.size(); i++){
                vector<AttributeNode*> tuple = Read_Data_File(dbname, schema, blocks[i] / getTupleSize(schema), 
                                                getTupleSize(schema));
                int index = 0;
                bool ok = true;
                for(int k = 0; k < (int)tuple.size() && index < (int)arr.size(); k++){
                    if(k == positions[index]){
                        if(tuple[k]->index == 0){
                            if(tuple[k]->num == stoi(arr[index].second)){
                                index++;
                                // cout << "reached" << endl;
                            }
                            else{
                                ok = false;
                                break;
                            }
                        }
                        else if(tuple[k]->index == 1){
                            if(tuple[k]->b == stoi(arr[index].second)){
                                index++;
                            }
                            else{
                                ok = false;
                                break;
                            }
                        }
                        else{
                            if(tuple[k]->str == arr[index].second){
                                index++;
                            }
                            else{
                                ok = false;
                                break;
                            }
                        }
                    }
                }
                if(ok){
                    res.push_back(blocks[i]);
                }
                FreeTuple(tuple);
            }
            return res;
        }
    }
    return {};
}

int Delete_Inner(char* dbname, char* meta, vector<pair<string,string>> schema, int i){
    int tupleNum = i / getTupleSize(schema), 
    tupleSize = getTupleSize(schema);
    vector<AttributeNode*> tuple = Read_Data_File(dbname, schema, tupleNum, tupleSize);
    for(int j = 0; j < (int)tuple.size(); j++){
        int bs = BinarySearch(j);
        if(bs == -1) continue;
        int root = Read_4Bytes_Address(names[bs], 4);
        Delete_AVLBlock(names[bs], root, i, tuple[j]->num, index_attr[bs]);
    }
    Delete_Data_Tuple(dbname, meta, schema, tupleNum, tupleSize);
    
    tuple = Read_Data_File(dbname, schema, tupleNum, tupleSize);
    for(int j = 0; j < (int)tuple.size(); j++){
        int bs = BinarySearch(j);
        if(bs == -1) continue;
        int root = Read_4Bytes_Address(names[bs], 4);
        int avlloc = Search_INT_AVLindex(names[bs], root, -1, tuple[j]->num, index_attr[bs]);
        int oldblock = Read_Last_Tuple_Address(meta);
        Replace_AVLBlock(names[bs], avlloc + 5 * INTEGER, oldblock, i);
        if(avlnodes.find({index_attr[bs], avlloc}) != avlnodes.end())
            avlnodes.erase({index_attr[bs], avlloc});
    }
    return 1;
}

int Delete_In_Database(char* name, char* dbname, char* meta, vector<pair<string,string>> schema, vector<pair<string,string>> arr){
    vector<int> blocks = Search_Database(name, dbname, meta, schema, arr);
    int deleted = 0;
    while((int)blocks.size() > 0){
        Delete_Inner(dbname, meta, schema, blocks[0]);
        blocks = Search_Database(name, dbname, meta, schema, arr);
        deleted++;
    }
    return deleted;
}

void Update_Attr(AttributeNode* node, string val){
    if(node->index == 0)
        node->num = stoi(val);
    else if(node->index == 1)
        node->b = stoi(val);
    else
        strcpy(node->str, val.c_str());
}

int Update_In_Database(char* name, char* dbname, char* meta, vector<pair<string,string>> schema, vector<pair<string,string>> arr1,
                        vector<pair<string,string>> arr2){
    vector<int> blocks = Search_Database(name, dbname, meta, schema, arr1);
    vector<int> positions = GetPositions(schema, arr2);
    int tupleSize = getTupleSize(schema);
    int updated = 0, loop = 0;
    while((int)blocks.size() > 0 && loop < 10){
        int tupleNum = blocks[0] / tupleSize;
        vector<AttributeNode*> tuple = Read_Data_File(dbname, schema, tupleNum, tupleSize);
        Delete_Inner(dbname, meta, schema, blocks[0]);
        int index = 0;
        for(int j = 0; j < (int)tuple.size() && index < (int)positions.size(); j++){
            if(j == positions[index]){
                Update_Attr(tuple[j], arr2[index].second);
                index++;
            }
        }
        int blockaddr = Write_In_Data_File(dbname, meta, tuple, tupleSize);
        cout << "Written in Data File" << endl;
        Update_Index(name, meta, tuple, blockaddr);
        cout << "Updated Index File" << endl;
        FreeTuple(tuple);
        blocks = Search_Database(name, dbname, meta, schema, arr1);
        updated++;
        loop++;
    }
    return updated;
}

vector<vector<int>> arr(3, vector<int>(1e5 + 1, 0));

int getNextNum(int i){
    int r = rand();
    while(arr[i][r] > 10){
        cout << "r: " << r << endl;
        r = rand();
    }
    return r;
}

void test(){
    string name = "abcdefgh";
    cout << "Enter Number of tuples: ";
    int MAXN;
    cin >> MAXN;
    // vector<int> roll(100000 + 1, 0);
    // vector<int> sal(100000 + 1, 0);
    // vector<int> pin(100000 + 1, 0);
  auto start = high_resolution_clock::now();
    char* dbname = (char*) malloc(STRING * sizeof(char));
    char* meta = (char*) malloc(STRING * sizeof(char));
    char* temp = (char*) malloc(STRING * sizeof(char));
    strcpy(dbname, "Test.db");
    strcpy(meta, "Test.meta");
    strcpy(temp, "Test");
    int r = 500, index = 0;
    for(int i = 0; i < MAXN; i++){
        // cout << "i: " << i << endl;
        if(index >= 5){
            r++;
            index = 0;
        }
        // cout << "i: " << i << endl;
        int s = 2;
        // cout << "i: " << i << endl;
        int p = 3;
        // roll[r]++;
        // sal[s]++;
        // pin[p]++;
        // int r = 100;
        // int s = 200;
        // int p = 300;
        arr[0][r]++;
        arr[1][s]++;
        arr[2][p]++;
        index++;
        // cout << "i: " << i << endl;
        AttributeNode* node1 = getAttributeNode(0, true, name, 2);
        // cout << "i: " << i << endl;
        AttributeNode* node2 = getAttributeNode(r, true, name, 0);
        // // cout << "i: " << i << endl;
        AttributeNode* node3 = getAttributeNode(s, true, name, 0);
        // // cout << "i: " << i << endl;
        AttributeNode* node4 = getAttributeNode(p, true, name, 0);
        // cout << "i: " << i << endl;
        vector<AttributeNode*> tuple(4);
        tuple[0] = node1;
        tuple[1] = node2;
        tuple[2] = node3;
        tuple[3] = node4;
        int blockAddr = Write_In_Data_File(dbname, meta, tuple, 44);
        // cout << "i: " << i << endl;
        Update_Index(temp, meta, tuple, blockAddr);
        // FreeTuple(tuple);
        // next_permutation(name.begin(), name.end());
        if(i % 100 == 0) cout << "i: " << i << endl;
    }
  auto stop = high_resolution_clock::now(); 
  
    // Get duration. Substart timepoints to  
    // get durarion. To cast it to proper unit 
    // use duration cast method 
    auto duration = duration_cast<microseconds>(stop - start);
    cout << "done in: " << duration.count() / 1000 << endl;
    // while(1){
    //     cout << rand() << endl;
    // }
}

void test1(){
    char* meta = (char*) malloc(STRING * sizeof(char));
    strcpy(meta, "Test.meta");
    int n;
    cin >> n;
    while(n--){
        int lta = Read_Last_Tuple_Address(meta);
        int dbs = Read_Database_Size(meta);
        int num = Read_Num_Tuples(meta);
        int ts = Read_Tuple_Size(meta);
        cout << "Before" << endl;
        cout << lta << " " << dbs << " " << num << " " << ts << endl;
        Update_Last_Tuple_Address(meta, lta + 44);
        ts = Read_Tuple_Size(meta);
        cout << "tuple size: " << ts << endl;
        Update_Database_Size(meta, dbs + 44);
        ts = Read_Tuple_Size(meta);
        cout << "tuple size: " << ts << endl;
        Update_Num_Tuples(meta, num + 1);
        ts = Read_Tuple_Size(meta);
        cout << "tuple size: " << ts << endl;
        lta = Read_Last_Tuple_Address(meta);
        dbs = Read_Database_Size(meta);
        num = Read_Num_Tuples(meta);
        ts = Read_Tuple_Size(meta);
        cout << "After" << endl;
        cout << lta << " " << dbs << " " << num << " " << ts << endl;
    }
}

void test2(){
    char* meta = (char*) malloc(STRING * sizeof(char));
    strcpy(meta, "Test.meta");
    create_file(meta);
    int t;
    cin >> t;
    int n = 100;
    while(t--){
        int fd = open(meta, O_WRONLY);
        write(fd, (void*)&n, 4);
        close(fd);
        n += 100;
    }
    int fd = open(meta, O_RDONLY);
    void* buf = malloc(4);
    read(fd, buf, 4);
    cout << *((int*)buf) << endl;
    close(fd);
}

int main(){
    // int i;
    // cout << sizeof(i) << endl;
    // test2();
    /**
     * Example schema = name(string), roll(int), salary(int), pincode(int)
     * 
     * schema creation syntax
     * 1 create schema database_name(attr type, attr type, ......)
     * Ex. = 1 create schema Person(name string, roll int, salary int, pincode int)
     * After creation put indexing details if needed.
     * 
     * insert syntax
     * 2 database_name attr1value attr2value attr3value ........
     * 2 Person Rahul 24 50000 833201
     * 
     * print all tuples syntax
     * 3 database_name
     * 3 Person
     * 
     * print meta and schema file contents syntax
     * 4 database_name
     * 4 Person
     * 
     * Put Indexing details
     * 6 database_name attr0 attr1 ..... 
     * 6 Person 1 2 3
     * The above means I want indexes for attributes at position 1(roll), 2(salary), 3(pincode);
     * Indexing only works for INTEGER attributes
     * 
     * Search database
     * 7 database_name attr1 value attr2 value ....
     * 7 Person roll 24 OR
     * 7 Person salary 20000 OR
     * 7 Person roll 24 salary 20000
     * 
     * Delete database
     * 8 database_name
     * 8 Person
     * 
     * Delete from database syntax
     * d database_name attr1 value1 attr2 value .....
     * d Person roll 24 salary 50000 
     * This means delete all tuples whose roll = 24 and salary = 50000
     * 
     * Update database syntax
     * u database_name attr1 value1 attr2 value2 ....., attr1 value1 attr2 value2 ..... 
     * u Person roll 24 salary 50000, roll 22
     * This means update roll to 24 and salary to 50000 where roll = 22
     * 
     **/
    while(1){
        cout << "Enter query: ";
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
            PrintLines(LINEHEIGHT);
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


            Initialize_AVLindex(name);


            cout << "created avl index file" << endl;
            strcpy(name, (dbname + ".btree").c_str());
            create_file(name);
            cout << "created B-Tree index file" << endl;
            PrintLines(LINEHEIGHT);
            free(name);
        }
        else if(query[0] == '2'){
            int sp = query.find(' ') + 1;
            string dbname = query.substr(sp, query.find(' ', sp) - sp);
            char* name = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            strcpy(name, (dbname + ".schema").c_str());
            vector<pair<string,string>> schema = Read_Schema_File(name);
            vector<AttributeNode*> tuple = getTuple(schema, query.substr(query.find(' ', sp) + 1, query.size() - query.find(' ', sp)));
            int tupleSize = getTupleSize(schema);
            strcpy(name, (dbname + ".db").c_str());
            strcpy(meta, (dbname + ".meta").c_str());
            int blockAddr = Write_In_Data_File(name, meta, tuple, tupleSize);
            strcpy(name, dbname.c_str());
            Update_Index(name, meta, tuple, blockAddr);
            free(name);
            free(meta);
            FreeTuple(tuple);
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
            int tot = 0;
            PrintLines(LINEHEIGHT);
            PrintUnderScore((int)schema.size() * (int)SPACE);
            PrintSchema(schema);
            PrintUnderScore((int)schema.size() * (int)SPACE);
            for(int i = 0; i < size; i++){
                vector<AttributeNode*> tuple = Read_Data_File(name, schema, i, getTupleSize(schema));
                PrintTuple(tuple);
                PrintUnderScore((int)schema.size() * (int)SPACE);
                FreeTuple(tuple);
                tot++;
            }
            PrintLines(LINEHEIGHT);
            cout << "Total read: " << tot << endl;
            PrintLines(LINEHEIGHT);
            free(name);
            free(meta);
        }
        else if(query[0] == '4'){
            string dbname = query.substr(query.find(' ') + 1, query.size() - query.find(' '));
            char* meta = (char*) malloc(STRING * sizeof(char));
            strcpy(meta, (dbname + ".meta").c_str());
            int lta = Read_Last_Tuple_Address(meta);
            int dbsize = Read_Database_Size(meta);
            int numtuples = Read_Num_Tuples(meta);
            string name = Read_DatabaseName(meta);
            int tupleSize = Read_Tuple_Size(meta);
            PrintLines(LINEHEIGHT);
            cout << "Last tuple address:       " << lta << endl;
            cout << "Database size:            " << dbsize << endl;
            cout << "Number of Tuples:         " << numtuples << endl;
            cout << "Tuple Size:               " << tupleSize << endl;
            cout << "Database name:            " << name << endl;
            PrintLines(LINEHEIGHT);
            strcpy(meta, (dbname + ".schema").c_str());
            vector<pair<string,string>> schema = Read_Schema_File(meta);
            PrintSchemaWithType(schema);
            PrintLines(LINEHEIGHT);
            free(meta);
        }
        else if(query[0] == '5'){
            // int fsp = query.find(' '), ssp = query.find(' ', fsp + 1);
            // string dbname = query.substr(fsp + 1, ssp - fsp - 1);
            // int tupleNum = stoi(query.substr(ssp + 1, query.size() - ssp));
            // // cout << dbname << " " << tupleNum << endl;
            // char* name = (char*) malloc(STRING * sizeof(char));
            // char* meta = (char*) malloc(STRING * sizeof(char));
            // strcpy(name, (dbname + ".schema").c_str());
            // strcpy(meta, (dbname + ".meta").c_str());
            // vector<pair<string,string>> schema = Read_Schema_File(name);
            // strcpy(name, (dbname + ".db").c_str());
            // Delete_Data_Tuple(name, meta, schema, tupleNum, Read_Tuple_Size(meta));
        }
        else if(query[0] == '6'){
            string dbname = "";
            int i = 2;
            while(i < (int)query.size() && query[i] != ' '){
                dbname += query[i];
                i++;
            }
            vector<int> arr;
            i++;
            string curr = "";
            while(i < (int)query.size()){
                if(query[i] == ' '){
                    arr.push_back(stoi(curr));
                    curr = "";
                }
                else curr += query[i];
                i++;
            }
            if(curr != "") arr.push_back(stoi(curr));
            char* name = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            strcpy(meta, (dbname + ".meta").c_str());
            strcpy(name, (dbname).c_str());
            Update_Num_Index(meta, (int)arr.size());
            Write_Index(meta, arr);
            // cout << dbname << endl;
            arr = Read_Index(meta);
            // for(int i: arr) cout << i << " ";
            // cout << endl;
            Initialize_DB(name, meta);
            InitializeIndexes();
            PrintLines(LINEHEIGHT);
            cout << "Index details saved" << endl;
            PrintLines(LINEHEIGHT);
            free(name);
            free(meta);
        }
        else if(query[0] == '7'){
            string dbname = "";
            int i = 2;
            while(i < (int)query.size() && query[i] != ' '){
                dbname += query[i];
                i++;
            }
            i++;
            string curr = "";
            bool t = true;
            vector<pair<string,string>> arr;
            while(i < (int)query.size()){
                if(query[i] == ' '){
                    if(t) arr.push_back({ curr, "" });
                    else arr[(int)arr.size() - 1].second = curr;
                    curr = "";
                    t = !t;
                }
                else curr += query[i];
                i++;
            }
            if((int)arr.size() > 0) arr[(int)arr.size() - 1].second = curr;
            for(pair<string, string> p: arr){
                cout << p.first << " " << p.second << endl;
            }
            char* schname = (char*) malloc(STRING * sizeof(char));
            char* name = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            char* temp = (char*) malloc(STRING * sizeof(char));
            strcpy(schname, (dbname + ".schema").c_str());
            strcpy(name, (dbname + ".db").c_str());
            strcpy(meta, (dbname + ".meta").c_str());
            strcpy(temp, (dbname).c_str());
            vector<pair<string, string>> schema = Read_Schema_File(schname);
            vector<int> blocks = Search_Database(temp, name, meta, schema, arr);
            int tot = 0;
            PrintLines(LINEHEIGHT);
            PrintUnderScore((int)schema.size() * (int)SPACE);
            PrintSchema(schema);
            PrintUnderScore((int)schema.size() * (int)SPACE);
            for(int j: blocks){
                vector<AttributeNode*> tuple = Read_Data_File(name, schema, j / getTupleSize(schema), getTupleSize(schema));
                PrintTuple(tuple);
                PrintUnderScore((int)schema.size() * (int)SPACE);
                FreeTuple(tuple);
                tot++;
            }
            PrintLines(LINEHEIGHT);
            cout << "Total Read " << tot << endl;
            PrintLines(LINEHEIGHT);
            free(schname);
            free(name);
            free(meta);
            free(temp);
        }
        else if(query[0] == 'd'){
            string dbname = "";
            int i = 2;
            while(i < (int)query.size() && query[i] != ' '){
                dbname += query[i];
                i++;
            }
            i++;
            string curr = "";
            bool t = true;
            vector<pair<string,string>> arr;
            while(i < (int)query.size()){
                if(query[i] == ' '){
                    if(t) arr.push_back({ curr, "" });
                    else arr[(int)arr.size() - 1].second = curr;
                    curr = "";
                    t = !t;
                }
                else curr += query[i];
                i++;
            }
            if((int)arr.size() > 0) arr[(int)arr.size() - 1].second = curr;
            // for(pair<string, string> p: arr){
            //     cout << p.first << " " << p.second << endl;
            // }
            char* name = (char*) malloc(STRING * sizeof(char));
            char* db = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            char* schname = (char*) malloc(STRING * sizeof(char));
            strcpy(name, dbname.c_str());
            strcpy(db, (dbname + ".db").c_str());
            strcpy(meta, (dbname + ".meta").c_str());
            strcpy(schname, (dbname + ".schema").c_str());
            vector<pair<string,string>> schema = Read_Schema_File(schname);
            int deleted = Delete_In_Database(name, db, meta, schema, arr);
            PrintLines(LINEHEIGHT);
            cout << "Deleted " << deleted << " records" << endl;
            PrintLines(LINEHEIGHT);
            free(name);
            free(db);
            free(meta);
            free(schname);
        }
        else if(query[0] == 'u'){
            string dbname = "";
            int i = 2;
            while(i < (int)query.size() && query[i] != ' '){
                dbname += query[i];
                i++;
            }
            i++;
            string curr = "";
            bool t = true;
            vector<pair<string,string>> arr, arr1;
            while(i < (int)query.size() && query[i] != ','){
                if(query[i] == ' '){
                    if(t) arr.push_back({ curr, "" });
                    else arr[(int)arr.size() - 1].second = curr;
                    curr = "";
                    t = !t;
                }
                else curr += query[i];
                i++;
            }
            if((int)arr.size() > 0) arr[(int)arr.size() - 1].second = curr;

            // for(pair<string, string> p: arr){
            //     cout << p.first << " " << p.second << endl;
            // }

            i += 2;
            t = true;
            curr = "";

            while(i < (int)query.size() && query[i] != ','){
                if(query[i] == ' '){
                    if(t) arr1.push_back({ curr, "" });
                    else arr1[(int)arr1.size() - 1].second = curr;
                    curr = "";
                    t = !t;
                }
                else curr += query[i];
                i++;
            }
            if((int)arr1.size() > 0) arr1[(int)arr1.size() - 1].second = curr;
            for(pair<string, string> p: arr1){
                cout << p.first << " " << p.second << endl;
            }
            char* name = (char*) malloc(STRING * sizeof(char));
            char* db = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            char* schname = (char*) malloc(STRING * sizeof(char));
            strcpy(name, dbname.c_str());
            strcpy(db, (dbname + ".db").c_str());
            strcpy(meta, (dbname + ".meta").c_str());
            strcpy(schname, (dbname + ".schema").c_str());
            vector<pair<string,string>> schema = Read_Schema_File(schname);
            int updated = Update_In_Database(name, db, meta, schema, arr1, arr);
            PrintLines(LINEHEIGHT);
            cout << "Updated some records" << endl;
            PrintLines(LINEHEIGHT);
            free(name);
            free(db);
            free(meta);
            free(schname);
        }
        else if(query[0] == '8'){
            string dbname = "";
            int i = 2;
            while(i < (int)query.size() && query[i] != ' '){
                dbname += query[i];
                i++;
            }
            char* name = (char*) malloc(STRING * sizeof(char));
            char* dbbname = (char*) malloc(STRING * sizeof(char));
            char* meta = (char*) malloc(STRING * sizeof(char));
            char* schname = (char*) malloc(STRING * sizeof(char));
            strcpy(name, dbname.c_str());
            strcpy(dbbname, (dbname + ".db").c_str());
            strcpy(meta, (dbname + ".meta").c_str());
            strcpy(schname, (dbname + ".schema").c_str());
            DropDatabase(name, dbbname, meta, schname);
            free(name);
            free(dbbname);
            free(meta);
            free(schname);
            PrintLines(LINEHEIGHT);
            cout << "Deleted entire Database" << endl;
            PrintLines(LINEHEIGHT);
        }
        else if(query[0] == '9'){
            cout << (int)names.size() << endl;
            for(int i = 0; i < (int)names.size(); i++){
                cout << "inorder for index: " << names[i] << endl;
                int root = Read_4Bytes_Address(names[i], 4);
                inorder(names[i], root);
            }
        }
        else if(query[0] == '0'){
            char* name = (char*) malloc(STRING * sizeof(char));
            strcpy(name, "Test1.avl");
            test();
            // int root = Read_4Bytes_Address(name, 4);
            // root = Delete_INT_AVLindex(name, root, -1, 3, 1);
            // Update_Root_Address(name, root);
            // test1(name);
            // int root = Read_4Bytes_Address(name, 4);
            // Update_AVLHeaders(name, root + INTEGER, {10, -1, -1, 10});
            // inorder(name, root);
        }
        // else if(query[0] == '6'){
        //     char* name = (char*) malloc(STRING * sizeof(char));
        //     strcpy(name, "Student.avl");
        //     test1(name);
        // }
        // else if(query[0] == '7'){
        //     cout << "Enter key: " << endl;
        //     int key;
        //     cin >> key;
        //     char* name = (char*) malloc(STRING * sizeof(char));
        //     strcpy(name, "Student.avl");
        //     int root = Read_4Bytes_Address(name, 4);
        //     root = Delete_INT_AVLindex(name, root, -1, key);
        //     Update_Root_Address(name, root);
        // }
        // else if(query[0] == '8'){
        //     cout << "Inorder" << endl;
        //     char* name = (char*) malloc(STRING * sizeof(char));
        //     strcpy(name, "Student.avl");
        //     inorder(name, Read_4Bytes_Address(name, 4));
        // }
        // else if(query[0] == '9'){
        //     // test2();
        // }
    }
}
