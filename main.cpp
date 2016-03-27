#include "common.h"
#include <glog/logging.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <cctype>
// #include <thread>

std::unique_ptr< UserDB >        g_pUserDB;
std::unique_ptr< ItemDB >        g_pItemDB;
std::unique_ptr< InteractionStore > g_InteractStore;
uint32_t                         g_nMaxUserID = 0;
uint32_t                         g_nMaxItemID = 0;
uint32_t                         g_nMaxThread = 1;

// for test
static void handle_command();
static void print_data_info();
static void gen_small_dataset( uint32_t nUsers, uint32_t nItems );
static void test();
static void test1();

namespace std {
    ostream& operator << ( ostream &os, const User &user )
    {
        os << "User: " << user.ID() << endl;
        os << "JobRoles: ";
        print_container( os, user.jobRoles() );
        os << "CareerLevel: " << user.careerLevel() << endl;
        os << "DiscplineID: " << user.discplineID() << endl;
        os << "IndustryID: " << user.industryID() << endl;
        os << "Country: " << user.country() << endl;
        os << "Region: " << user.region() << endl;
        os << "NumOfCVEntry: " << user.numOfCvEntry() << endl;
        os << "YearsOfExperience: " << user.yearsOfExperience() << endl;
        os << "YearsOfCurrentJob: " << user.yearsOfCurrentJob() << endl;
        os << "EduDegree: " << user.eduDegree() << endl;
        os << "EduFields: ";
        print_container( os, user.eduFields() );

        uint32_t nTotalActioned = 0;
        // Clicked
        {
            const InteractionVector &v = user.interactionVector( CLICK );
            os << v.size() << " Clicked items: ";
            nTotalActioned += v.size();
            for( auto it = v.begin(); it != v.end(); ++it ) {
                auto sp = it->lock();
                cout << sp->itemID() << "@" << sp->time() << " ";
            } // for
            os << endl;
        }

        // Bookmarked
        {
            const InteractionVector &v = user.interactionVector( BOOKMARK );
            os << v.size() << " Bookmarked items: ";
            nTotalActioned += v.size();
            for( auto it = v.begin(); it != v.end(); ++it ) {
                auto sp = it->lock();
                cout << sp->itemID() << "@" << sp->time() << " ";
            } // for
            os << endl;
        }

        // Replied
        {
            const InteractionVector &v = user.interactionVector( REPLY );
            os << v.size() << " Replied items: ";
            nTotalActioned += v.size();
            for( auto it = v.begin(); it != v.end(); ++it ) {
                auto sp = it->lock();
                cout << sp->itemID() << "@" << sp->time() << " ";
            } // for
            os << endl;
        }

        // Deleted
        {
            const InteractionVector &v = user.interactionVector( DELETE );
            os << v.size() << " Deleted items: ";
            nTotalActioned += v.size();
            for( auto it = v.begin(); it != v.end(); ++it ) {
                auto sp = it->lock();
                cout << sp->itemID() << "@" << sp->time() << " ";
            } // for
            os << endl;
        }

        os << "Totally " << nTotalActioned << " items performed actions by this user." << endl;

        return os;
    }

    ostream& operator << ( ostream &os, const Item &item )
    {
        os << "Item: " << item.ID() << endl;
        os << "Title: ";
        print_container( os, item.title() );
        os << "CareerLevel: " << item.careerLevel() << endl;
        os << "DiscplineID: " << item.discplineID() << endl;
        os << "IndustryID: " << item.industryID() << endl;
        os << "Country: " << item.country() << endl;
        os << "Region: " << item.region() << endl;
        os << "latitude: " << item.latitude() << endl;
        os << "longitude: " << item.longitude() << endl;
        os << "EmploymentType: " << item.employmentType() << endl;
        os << "tags: ";
        print_container( os, item.tags() );
        os << "createTime: " << item.createTime() << endl;
        os << "Active: " << (item.isActive() ? "Yes" : "No") << endl;

        uint32_t nTotalActioned = 0;
        // clicked
        {
            const InteractionVector &v = item.interactionVector( CLICK );
            os << v.size() << " users(times) clicked: ";
            nTotalActioned += v.size();
            for( auto it = v.begin(); it != v.end(); ++it ) {
                auto sp = it->lock();
                cout << sp->userID() << "@" << sp->time() << " ";
            } // for
            os << endl;
        }

        // bookmarked
        {
            const InteractionVector &v = item.interactionVector( BOOKMARK );
            os << v.size() << " users(times) bookmarked: ";
            nTotalActioned += v.size();
            for( auto it = v.begin(); it != v.end(); ++it ) {
                auto sp = it->lock();
                cout << sp->userID() << "@" << sp->time() << " ";
            } // for
            os << endl;
        }

        // replied
        {
            const InteractionVector &v = item.interactionVector( REPLY );
            os << v.size() << " users(times) replied: ";
            nTotalActioned += v.size();
            for( auto it = v.begin(); it != v.end(); ++it ) {
                auto sp = it->lock();
                cout << sp->userID() << "@" << sp->time() << " ";
            } // for
            os << endl;
        }

        // deleted
        {
            const InteractionVector &v = item.interactionVector( DELETE );
            os << v.size() << " users(times) deleted: ";
            nTotalActioned += v.size();
            for( auto it = v.begin(); it != v.end(); ++it ) {
                auto sp = it->lock();
                cout << sp->userID() << "@" << sp->time() << " ";
            } // for
            os << endl;
        }

        os << "Totally performed action by " << nTotalActioned << " users(times)." << endl;

        return os;
    }
} // namespace std


static
bool read_uint_set( char *str, UIntSet &uintSet )
{
    uint32_t id;
    char *saveEnd2 = NULL;
    for( char *p = strtok_r(str, ",", &saveEnd2); p; p = strtok_r(NULL, ",", &saveEnd2) ) {
        read_from_string(p, id);
        if( id )
            uintSet.insert( id );
    } // for
    return true;
}

/*
 * processLine 或者用值传入，或者用 const ref 传入，
 * 但不可以用普通引用传入。
 */
static
void load_file_thread_routine( std::ifstream &inFile, boost::mutex &fileMtx,
                const uint32_t BATCH_SIZE, uint32_t &lineno,
                const std::function< void(std::string&, uint32_t) > &processLine )
{
    using namespace std;

    int i = 0, j = 0;
    vector< string > lines( BATCH_SIZE );
    vector< uint32_t > lineIDs( BATCH_SIZE );

    while (true) {
        boost::unique_lock< boost::mutex >  lock(fileMtx);
        for( i = 0; i < BATCH_SIZE; ++i ) {
            if( !getline(inFile, lines[i]) )
                break;
            lineIDs[i] = ++lineno;
        } // for
        lock.unlock();

        for( j = 0; j < i; ++j ) {
            processLine( lines[j], lineIDs[j] );
        } // for

        if( i < BATCH_SIZE )   // getline fail, eof or filestream fail
            break;   // jump out while true
    } // while

    return;
}

static
void load_user_data( const char *filename )
{
    using namespace std;

    ifstream inFile( filename, ios::in );
    boost::mutex  fileMtx;
    const uint32_t  BATCH_SIZE = 100;   // 每个线程一次处理行数
    uint32_t lineno = 0;

    if( !inFile )
        throw runtime_error( "Cannot open user data file!" );

    // skip the title
    string title;
    getline( inFile, title );
    if( !inFile )
        throw runtime_error( "Invalid user data format!" );

    auto processLine = []( string &line, uint32_t lineCount ) {
        char *pField = NULL, *saveEnd1 = NULL; // for strtok_r
        char errstr[128];
        User_sptr pUser = std::make_shared< User >();
        char *pLine = const_cast<char*>(line.c_str());

        // read ID, maybe empty line, so when read fail just skip
        if( !(pField = strtok_r(pLine, "\t", &saveEnd1)) || !read_from_string(pField, pUser->ID()) )
            return;
        // job roles
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_uint_set(pField, pUser->jobRoles()) ) {
            sprintf(errstr, "error reading %u record's jobrole!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // career level
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pUser->careerLevel()) ) {
            sprintf(errstr, "error reading %u record careerLevel!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        LOG_IF(WARNING, pUser->careerLevel() > 6) << pUser->careerLevel()
                << " is not a valid careerLevel value, record no: " << lineCount;
        // discplineID
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pUser->discplineID()) ) {
            sprintf(errstr, "error reading %u record discplineID!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // industryID
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pUser->industryID()) ) {
            sprintf(errstr, "error reading %u record industryID!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // country
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pUser->country()) ) {
            sprintf(errstr, "error reading %u record country!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // region
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pUser->region()) ) {
            sprintf(errstr, "error reading %u record region!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        LOG_IF(WARNING, pUser->region() > 16) << pUser->region()
                << " is not a valid region value, record no: " << lineCount;
        // CV entry
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pUser->numOfCvEntry()) ) {
            sprintf(errstr, "error reading %u record numOfCvEntry!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        LOG_IF(WARNING, pUser->numOfCvEntry() > 3) << pUser->numOfCvEntry()
                << " is not a valid numOfCvEntry value, record no: " << lineCount;
        // yearsOfExperience
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pUser->yearsOfExperience()) ) {
            sprintf(errstr, "error reading %u record yearsOfExperience!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        LOG_IF(WARNING, pUser->yearsOfExperience() > 7) << pUser->yearsOfExperience()
                << " is not a valid yearsOfExperience value, record no: " << lineCount;
        // yearsOfCurrentJob
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pUser->yearsOfCurrentJob()) ) {
            sprintf(errstr, "error reading %u record yearsOfCurrentJob!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        LOG_IF(WARNING, pUser->yearsOfCurrentJob() > 7) << pUser->yearsOfCurrentJob()
                << " is not a valid yearsOfCurrentJob value, record no: " << lineCount;
        // eduDegree
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pUser->eduDegree()) ) {
            sprintf(errstr, "error reading %u record eduDegree!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        LOG_IF(WARNING, pUser->eduDegree() > 3) << pUser->eduDegree()
                << " is not a valid eduDegree value, record no: " << lineCount;
        // eduFields, if eduDegree is 0, eduFields can be empty
        if( (pField = strtok_r(NULL, "\t", &saveEnd1)) ) {
            read_uint_set(pField, pUser->eduFields());
        } // if

        // cout << *pUser << endl;
        g_pUserDB->addUser( pUser );
        g_nMaxUserID = pUser->ID() > g_nMaxUserID ? pUser->ID() : g_nMaxUserID;
    }; // end lambda

    boost::thread_group thrgroup;
    for( uint32_t i = 0; i < g_nMaxThread; ++i )
        thrgroup.create_thread( std::bind(load_file_thread_routine,
                                    std::ref(inFile), std::ref(fileMtx),
                                    BATCH_SIZE, std::ref(lineno), std::ref(processLine)) );
        // thrgroup.create_thread( std::bind(load_file_thread_routine,
                                    // std::ref(inFile), std::ref(fileMtx),
                                    // BATCH_SIZE, std::ref(lineno), processLine) );
    thrgroup.join_all();

    return;
}

static
void load_item_data( const char *filename )
{
    using namespace std;

    ifstream inFile( filename, ios::in );
    boost::mutex  fileMtx;
    const uint32_t  BATCH_SIZE = 100;   // 每个线程一次处理行数
    uint32_t lineno = 0;

    if( !inFile )
        throw runtime_error( "Cannot open item data file!" );

    // skip the title line
    string title;
    getline( inFile, title );
    if( !inFile )
        throw runtime_error( "Invalid item data format!" );

    auto processLine = []( string &line, uint32_t lineCount ) {
        char *pField = NULL, *saveEnd1 = NULL; // for strtok_r
        char errstr[128];
        Item_sptr pItem = std::make_shared< Item >();
        char *pLine = const_cast<char*>(line.c_str());

        // read ID, maybe empty line, so when read fail just skip
        if( !(pField = strtok_r(pLine, "\t", &saveEnd1)) || !read_from_string(pField, pItem->ID()) )
            return;
        // read title
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_uint_set(pField, pItem->title()) ) {
            sprintf(errstr, "error reading %u record's title!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // career level
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pItem->careerLevel()) ) {
            sprintf(errstr, "error reading %u record careerLevel!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        LOG_IF(WARNING, pItem->careerLevel() > 6) << pItem->careerLevel()
                << " is not a valid careerLevel value, record no: " << lineCount;
        // discplineID
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pItem->discplineID()) ) {
            sprintf(errstr, "error reading %u record discplineID!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // industryID
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pItem->industryID()) ) {
            sprintf(errstr, "error reading %u record industryID!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // country
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pItem->country()) ) {
            sprintf(errstr, "error reading %u record country!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // region
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pItem->region()) ) {
            sprintf(errstr, "error reading %u record region!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        LOG_IF(WARNING, pItem->region() > 16) << pItem->region()
                << " is not a valid region value, record no: " << lineCount;
        // latitude
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pItem->latitude()) ) {
            sprintf(errstr, "error reading %u record latitude!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // longitude
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pItem->longitude()) ) {
            sprintf(errstr, "error reading %u record longitude!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // employmentType
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, pItem->employmentType()) ) {
            sprintf(errstr, "error reading %u record employmentType!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        LOG_IF(WARNING, pItem->employmentType() > 5) << pItem->employmentType()
                << " is not a valid employmentType value, record no: " << lineCount;
        // tags
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_uint_set(pField, pItem->tags()) ) {
            sprintf(errstr, "error reading %u record's tags!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        // timestamp
        unsigned long ts;
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, ts) ) {
            sprintf(errstr, "error reading %u record timestamp!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        pItem->createTime() = (time_t)ts;
        // active status
        int status;
        if( !(pField = strtok_r(NULL, "\t", &saveEnd1)) || !read_from_string(pField, status) ) {
            sprintf(errstr, "error reading %u record active status!", lineCount);
            LOG(WARNING) << errstr;
        } // if
        pItem->setActive( status ? true : false );

        // cout << *pItem << endl;
        g_pItemDB->addItem( pItem );
        g_nMaxItemID = pItem->ID() > g_nMaxItemID ? pItem->ID() : g_nMaxItemID;
    }; // end processLine

    boost::thread_group thrgroup;
    for( uint32_t i = 0; i < g_nMaxThread; ++i )
        thrgroup.create_thread( std::bind(load_file_thread_routine,
                                    std::ref(inFile), std::ref(fileMtx),
                                    BATCH_SIZE, std::ref(lineno), processLine) );
    thrgroup.join_all();

    return;
}

static
void load_interaction_data( const char *filename )
{
    using namespace std;

    ifstream inFile( filename, ios::in );
    boost::mutex  fileMtx;
    const uint32_t  BATCH_SIZE = 500;   // 每个线程一次处理行数
    uint32_t lineno = 0;

    if( !inFile )
        throw runtime_error( "Cannot open iteraction data file!" );

    // skip the title line
    string title;
    getline( inFile, title );
    if( !inFile )
        throw runtime_error( "Invalid interaction data format!" );

    auto processLine = []( string &line, uint32_t lineCount ) {
        uint32_t userID, itemID, interactType;
        unsigned long timestamp;
        InteractionRecord_sptr pInterRec;
        User_sptr pUser;
        Item_sptr pItem;

        stringstream str(line);
        str >> userID >> itemID >> interactType >> timestamp;
        assert( interactType < N_INTERACTION_TYPE );
        if ( !g_pUserDB->queryUser(userID, pUser) ) {
            LOG(INFO) << "load_interaction_data cannot find user: " << userID;
            return;
        } // if
        if ( !g_pItemDB->queryItem(itemID, pItem) ) {
            LOG(INFO) << "load_interaction_data cannot find item: " << itemID;
            return;
        } // if
        if ( (time_t)timestamp < pItem->createTime() ) {
            LOG(ERROR) << "Wrong interaction record " << lineCount << 
                    ": " << timestamp << " is earlier than item " << pItem->ID() 
                   << " created time " << pItem->createTime(); 
            return;
        } // if
        pInterRec = std::make_shared< InteractionRecord >
                           (pUser, pItem, interactType, timestamp);
        g_InteractStore->add( pInterRec );
        pUser->addInteraction( pInterRec );
        pItem->addInteraction( pInterRec );
    }; // end processLine

    boost::thread_group thrgroup;
    for( uint32_t i = 0; i < g_nMaxThread; ++i )
        thrgroup.create_thread( std::bind(load_file_thread_routine,
                                    std::ref(inFile), std::ref(fileMtx),
                                    BATCH_SIZE, std::ref(lineno), processLine) );
    thrgroup.join_all();

    // sort users' interactions and items' interaction, by time later to earlier
    cout << "Sorting interations by time......" << endl;
    auto sortInteractions = []( const InteractionRecord_wptr &pLeft,
                                    const InteractionRecord_wptr &pRight )->bool
    { return pLeft.lock()->time() > pRight.lock()->time(); };

    g_pUserDB->sortInteractions( sortInteractions );
    g_pItemDB->sortInteractions( sortInteractions );
}


static
void init()
{
    g_pUserDB.reset( new UserDB );
    g_pItemDB.reset( new ItemDB );
    g_InteractStore.reset( new InteractionStore(1000) );

    g_nMaxUserID = 0;
    g_nMaxItemID = 0;

    g_nMaxThread = boost::thread::hardware_concurrency();
    if( !g_nMaxThread )
        g_nMaxThread = 1;
}

int main( int argc, char **argv )
{
    using namespace std;

    google::InitGoogleLogging(argv[0]);

    try {
        // test();
        init();

        cout << "Loading users data..." << endl;
        load_user_data( "users.csv" );
        cout << "Loading items data..." << endl;
        load_item_data( "items.csv" );

        cout << "Loading interaction data..." << endl;
        load_interaction_data( "interactions.csv" );
        print_data_info();
        // gen_small_dataset( 80000, 100000 );
        handle_command();

    } catch ( const exception &ex ) {
        cerr << "Exception: " << ex.what() << endl;
        exit(-1);
    } // try

    cout << "Main program terminating......" << endl;
    return 0;
}

static
void print_user_info( uint32_t id )
{
    using namespace std;

    User_sptr pUser;
    bool result = g_pUserDB->queryUser( id, pUser );
    if( result )
        cout << *pUser << endl;
    else
        cout << "No user found for id: " << id << endl;
}

static
void print_item_info( uint32_t id )
{
    using namespace std;

    Item_sptr pItem;
    bool result = g_pItemDB->queryItem( id, pItem );
    if( result )
        cout << *pItem << endl;
    else
        cout << "No item found for id: " << id << endl;
}

static
void handle_command()
{
    using namespace std;

    string line;
    uint32_t id;
    char cmd;

    cout << "Please input command:" << endl;

    while( getline(cin, line) ) {
        stringstream str(line);
        str >> cmd >> id;
        cmd = tolower(cmd);
        if( 'q' == cmd )
            break;
        else if( 'u' == cmd )
            print_user_info( id );
        else if( 'i' == cmd )
            print_item_info( id );
        else {
            cout << "Invalid command!" << endl;
            continue;
        } // if
    } // while

    cout << "Command prompt quit." << endl;
}

static
void print_data_info()
{
    using namespace std;

    cout << "n_users: " << g_pUserDB->size() << endl;
    cout << "n_items: " << g_pItemDB->size() << endl;
    cout << "n_interactions: " << g_InteractStore->size() << endl;

    // for test
    cout << "g_nMaxUserID = " << g_nMaxUserID << endl;
    cout << "g_nMaxItemID = " << g_nMaxItemID << endl;
}

static
void gen_small_dataset( uint32_t nUsers, uint32_t nItems = 0 )
{
    using namespace std;

    const uint32_t MAXUID = g_nMaxUserID + 1;
    const uint32_t MAXIID = g_nMaxItemID + 1; 
    UIntSet users, items;
    User_sptr pUser;

    cout << "Generating small dataset......" << endl;

    // 对 interaction 排序，按时间由近及远
    auto cmpInteractByTime = []( const InteractionRecord_wptr pLeft, 
                                const InteractionRecord_wptr pRight )->bool
    { return pLeft.lock()->time() > pRight.lock()->time(); };

    std::multimap< uint32_t, InteractionRecord_wptr > interactions;

    auto createDataFile = []( const char *inFileName, const char *outFileName, const UIntSet &set ) {
        uint32_t id;
        string line;

        ifstream inFile( inFileName, ios::in );
        ofstream outFile( outFileName, ios::out );
        
        // copy the title line
        getline( inFile, line );
        outFile << line << endl;

        while (getline(inFile, line)) {
            if (sscanf( line.c_str(), "%u", &id ) != 1)
                continue;
            if (set.find(id) != set.end())
                outFile << line << endl;
        } // while
    };

    auto createInteractFile = [&]( const char *filename ) {
        // std::vector< InteractionRecord_wptr > arr( interactions.begin(), interactions.end() );
        std::vector< InteractionRecord_wptr > arr;
        arr.reserve( interactions.size() );
        for( const auto &v : interactions )
            arr.push_back( v.second );

        std::sort( arr.begin(), arr.end(), cmpInteractByTime );

        ofstream outFile( filename, ios::out );
        outFile << "user_id\titem_id\tinteraction_type\tcreated_at" << endl;

        for( const auto &v : arr ) {
            auto sp = v.lock();
            outFile << sp->user().lock()->ID() << "\t" << sp->item().lock()->ID() << "\t"
                << sp->type() << "\t" << sp->time() << endl;
        } // for
    };

    // 默认 item 小数据集是选取的 users 交互过的所有item
    auto dumpInteractedItemIDs = [&] {
        InteractionTable &iTable = pUser->interactionTable();        
        for( uint32_t i = 1; i < N_INTERACTION_TYPE; ++i ) {
            for( uint32_t j = 0; j < iTable[i].size(); ++j ) {
                uint32_t itemID = iTable[i][j].lock()->item().lock()->ID();
                items.insert( itemID );
                interactions.insert( std::make_pair( itemID, iTable[i][j] ) );
            } // for j
        } // for i
    };

    // 随机选取指定用户数，至少有一个 interaction 记录
    srand( time(0) );
    while( users.size() < nUsers ) {
        if( g_pUserDB->queryUser(rand() % MAXUID, pUser) && pUser->nInteractions() ) {
            users.insert( pUser->ID() );
            dumpInteractedItemIDs();
        } // if
    } // while

    createDataFile( "users.csv", "users_small.csv", users );
    createDataFile( "items.csv", "items_small1.csv", items );
    createInteractFile( "interactions_small1.csv" );

    // 按照上面的方法生成的 items 小数据集可能比需要的多，随机删除，直到指定数目。
    if( nItems ) {
        std::vector<uint32_t> arr( items.begin(), items.end() );
        while( arr.size() > nItems ) {
            uint32_t index = rand() % arr.size();
            std::vector<uint32_t>::iterator it = arr.begin() + index;
            interactions.erase( *it );
            arr.erase( it );
        } // while
        std::set<uint32_t> tmp(arr.begin(), arr.end());
        items.swap( tmp );
        createDataFile( "items.csv", "items_small2.csv", items );
        createInteractFile( "interactions_small2.csv" );
    } // if

    return;
}




static
void test1()
{
    using namespace std;

    // random query test
/*
 *     User_wptr wpUser = g_pUserDB->queryUser(20033);
 *     if( auto sp = wpUser.lock() )
 *         cout << *sp << endl;
 *
 *     Item_wptr wpItem = g_pItemDB->queryItem(422574);
 *     if( auto sp = wpItem.lock() )
 *         cout << *sp << endl;
 */
}


static
void test()
{
    using namespace std;

    /*
     * User user;
     * const char *sID = "2354";
     * from_str( sID, user.ID() );
     * cout << user.ID() << endl;
     */

/*
 *     multimap<int, string> dict;
 *     dict.insert( std::make_pair(1, "hello") );
 *     dict.insert( std::make_pair(1, "world") );
 *     dict.insert( std::make_pair(2, "test") );
 *
 *     for( auto it = dict.begin(); it != dict.end(); ++it )
 *         cout << it->first << " = " << it->second << endl;
 */

    /*
     * typedef int IntArr[100];
     * IntArr a;
     * cout << sizeof(a) << endl;
     */

    /*
     * User_wptr p = User_sptr();
     * if( p.lock() )
     *     cout << "p is valid" << endl;
     * else
     *     cout << "p is invalid" << endl;
     */

    exit(0);
}





