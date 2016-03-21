#include "common.h"
#include <glog/logging.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <thread>

std::unique_ptr< UserDB >        g_pUserDB;
std::unique_ptr< ItemDB >        g_pItemDB;

void test();
void test1();

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

        return os;
    }
} // namespace std


static
bool read_uint_set( char *str, std::set<uint32_t> &uintSet )
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

void load_user_data( const char *filename )
{
    using namespace std;

    char errstr[128];
    string line;
    char *pField = NULL, *saveEnd1 = NULL; // for strtok_r
    User_sptr pUser;
    ifstream inFile( filename, ios::in );

    if( !inFile )
        throw runtime_error( "Cannot open user data file!" );

    // skip the title line
    getline( inFile, line );
    if( !inFile )
        throw runtime_error( "Invalid user data format!" );

    uint32_t lineCount = 0;
    while( getline(inFile, line) ) {
        ++lineCount;
        char *pLine = const_cast<char*>(line.c_str());
        pUser.reset( new User );
        // read ID, maybe empty line, so when read fail just skip
        if( !(pField = strtok_r(pLine, "\t", &saveEnd1)) || !read_from_string(pField, pUser->ID()) )
            continue;
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
    } // while
}


void load_item_data( const char *filename )
{
    using namespace std;

    char errstr[128];
    string line;
    char *pField = NULL, *saveEnd1 = NULL; // for strtok_r
    Item_sptr pItem;
    ifstream inFile( filename, ios::in );

    if( !inFile )
        throw runtime_error( "Cannot open item data file!" );

    // skip the title line
    getline( inFile, line );
    if( !inFile )
        throw runtime_error( "Invalid item data format!" );

    uint32_t lineCount = 0;
    while( getline(inFile, line) ) {
        ++lineCount;
        char *pLine = const_cast<char*>(line.c_str());
        pItem.reset( new Item );
        // read ID, maybe empty line, so when read fail just skip
        if( !(pField = strtok_r(pLine, "\t", &saveEnd1)) || !read_from_string(pField, pItem->ID()) )
            continue;
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
    } // while
}


void load_interaction_data( const char *filename )
{
    using namespace std;

    char errstr[128];
    string line;
    uint32_t userID, itemID, interactType; 
    unsigned long timestamp;
    ifstream inFile( filename, ios::in );

    if( !inFile )
        throw runtime_error( "Cannot open iteraction data file!" );

    // skip the title line
    getline( inFile, line );
    if( !inFile )
        throw runtime_error( "Invalid interaction data format!" );

    uint32_t lineCount = 0;
    while( getline(inFile, line) ) {
        ++lineCount;
        stringstream str(line);
        str >> userID >> itemID >> interactType >> timestamp;
        if( str.fail() || str.bad() ) {
            LOG(WARNING) << "read interaction data " << lineCount << " line fail.";
            continue;
        } // if

        User_wptr wpUser = g_pUserDB->queryUser( userID );
        User_sptr spUser = wpUser.lock();
        if( !spUser ) {
            LOG(WARNING) << "No info about user " << userID << " in user database.";
            continue;
        } // if
        Item_wptr wpItem = g_pItemDB->queryItem( itemID );
        Item_sptr spItem = wpItem.lock();
        if( !spItem ) {
            LOG(WARNING) << "No info about item " << itemID << " in item database.";
            continue;
        } // if

        spUser->addInteractItem( wpItem, interactType, (time_t)timestamp );
        spItem->addInteractUser( wpUser, interactType, (time_t)timestamp );
    } // while
}


void init()
{
    g_pUserDB.reset( new UserDB );
    g_pItemDB.reset( new ItemDB );
}


int main( int argc, char **argv )
{
    using namespace std;

    google::InitGoogleLogging(argv[0]);

    try {
        // test();
        init();

        cout << "Loading users data and items data..." << endl;
        std::thread t1(load_user_data, "users_test.csv");
        std::thread t2(load_item_data, "items_test.csv");

        t1.join();
        t2.join();

        // test1();
        cout << "Loading interaction data..." << endl;
        load_interaction_data( "interactions_test.csv" );

    } catch ( const exception &ex ) {
        cerr << "Exception: " << ex.what() << endl;
        exit(-1);
    } // try

    return 0;
}




void test1()
{
    using namespace std;

    cout << "n_users: " << g_pUserDB->size() << endl;
    cout << "n_items: " << g_pItemDB->size() << endl;

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
