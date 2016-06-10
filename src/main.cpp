/*
 * GLOG_log_dir="." ./xing.bin
 * 暂不用考虑OpenMP版本的算法实现
 */
#include "common.h"
#include "recommend_algorithm.h"
#include <glog/logging.h>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <cassert>
#include <cctype>

#define    RECALL_SIZE 30

using std::cout; using std::endl;

std::unique_ptr< UserDB >        g_pUserDB;
std::unique_ptr< ItemDB >        g_pItemDB;
std::unique_ptr< InteractionStore > g_InteractStore;
uint32_t         g_nMaxUserID = 0;
uint32_t         g_nMaxItemID = 0;
uint32_t         g_nMaxThread = 1;

// test data
typedef std::set<uint32_t>            _IdSet;
typedef std::map<uint32_t, _IdSet>    TestDataSet;  // {uid: set(itemid 正反馈id列表)}
static TestDataSet                    g_TestData;

// for test
static void handle_command();
static void print_data_info();
// static void gen_small_dataset( uint32_t nUsers, uint32_t nItems );
static void test();
static void test1();

// 以下是打印 User Item Interaction 等类型数据， 用于测试
namespace std {
    ostream& operator << ( ostream &os, const User &user )
    {
        using namespace Test;
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
            uint32_t count = 0;
            const InteractionMap &m = user.interactionMap( CLICK );
            for ( const auto &mValue : m ) {
                const InteractionVector &v = mValue.second;
                count += v.size();
                for ( const auto sp : v ) {
                    // vValue is InteractionRecord_wptr
                    os << sp->itemID() << "@" << sp->time() << " ";
                } // for
            } // for
            os << count << " clicked items." << endl;
            nTotalActioned += count;
        }

        // Bookmarked
        {
            uint32_t count = 0;
            const InteractionMap &m = user.interactionMap( BOOKMARK );
            for ( const auto &mValue : m ) {
                const InteractionVector &v = mValue.second;
                count += v.size();
                for ( const auto sp : v ) {
                    // vValue is InteractionRecord_wptr
                    os << sp->itemID() << "@" << sp->time() << " ";
                } // for
            } // for
            os << count << " bookmarked items." << endl;
            nTotalActioned += count;
        }

        // Replied
        {
            uint32_t count = 0;
            const InteractionMap &m = user.interactionMap( REPLY );
            for ( const auto &mValue : m ) {
                const InteractionVector &v = mValue.second;
                count += v.size();
                for ( const auto sp : v ) {
                    // vValue is InteractionRecord_wptr
                    os << sp->itemID() << "@" << sp->time() << " ";
                } // for
            } // for
            os << count << " replied items." << endl;
            nTotalActioned += count;
        }

        // Deleted
        {
            uint32_t count = 0;
            const InteractionMap &m = user.interactionMap( DELETE );
            for ( const auto &mValue : m ) {
                const InteractionVector &v = mValue.second;
                count += v.size();
                for ( const auto sp : v ) {
                    // vValue is InteractionRecord_wptr
                    os << sp->itemID() << "@" << sp->time() << " ";
                } // for
            } // for
            os << count << " deleted items." << endl;
            nTotalActioned += count;
        }

        os << "Totally " << nTotalActioned << " items performed actions by this user." << endl;

        return os;
    }

    ostream& operator << ( ostream &os, const Item &item )
    {
        using namespace Test;
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
            uint32_t count = 0;
            const InteractionMap &m = item.interactionMap( CLICK );
            for ( const auto &mValue : m ) {
                const InteractionVector &v = mValue.second;
                count += v.size();
                for ( const auto sp : v ) {
                    // vValue is InteractionRecord_wptr
                    os << sp->userID() << "@" << sp->time() << " ";
                } // for
            } // for
            os << count << " clicked items." << endl;
            nTotalActioned += count;
        }

        // bookmarked
        {
            uint32_t count = 0;
            const InteractionMap &m = item.interactionMap( BOOKMARK );
            for ( const auto &mValue : m ) {
                const InteractionVector &v = mValue.second;
                count += v.size();
                for ( const auto sp : v ) {
                    // vValue is InteractionRecord_wptr
                    os << sp->userID() << "@" << sp->time() << " ";
                } // for
            } // for
            os << count << " bookmarked items." << endl;
            nTotalActioned += count;
        }

        // replied
        {
            uint32_t count = 0;
            const InteractionMap &m = item.interactionMap( REPLY );
            for ( const auto &mValue : m ) {
                const InteractionVector &v = mValue.second;
                count += v.size();
                for ( const auto sp : v ) {
                    // vValue is InteractionRecord_wptr
                    os << sp->userID() << "@" << sp->time() << " ";
                } // for
            } // for
            os << count << " replied items." << endl;
            nTotalActioned += count;
        }

        // deleted
        {
            uint32_t count = 0;
            const InteractionMap &m = item.interactionMap( DELETE );
            for ( const auto &mValue : m ) {
                const InteractionVector &v = mValue.second;
                count += v.size();
                for ( const auto sp : v ) {
                    // vValue is InteractionRecord_wptr
                    os << sp->userID() << "@" << sp->time() << " ";
                } // for
            } // for
            os << count << " deleted items." << endl;
            nTotalActioned += count;
        }

        os << "Totally performed action by " << nTotalActioned << " users(times)." << endl;

        return os;
    }
} // namespace std


// 将字符串中的一系列 uint 数据，逗号分隔，读入到set集合中
static
bool read_uint_set( char *str, UIntSet &uintSet )
{
    uint32_t id;
    char *saveEnd2 = NULL;
    bool ret = true;
    for( char *p = strtok_r(str, ",", &saveEnd2); p; p = strtok_r(NULL, ",", &saveEnd2) ) {
        if ( read_from_string(p, id) )
            uintSet.insert( id );
        else
            ret = false;
    } // for
    return ret;
}

/*
 * processLine 或者用值传入，或者用 const ref 传入，
 * 但不可以用普通引用传入。
 */
/**
 * @brief 用于多线程读入数据文件的线程函数
 *
 * @param inFile        数据文件对象
 * @param fileMtx       访问inFile的互斥锁
 * @param BATCH_SIZE    一次读入的行数
 * @param lineno        用于记录行号，传入之前设为0
 * @param processLine   处理读入行的回调函数，因数据文件而异，比如读入users.csv，
 *                      processLine 应该是根据读入行文本建立新的User信息并存入数据库
 */
static
void load_file_thread_routine( std::ifstream &inFile, boost::mutex &fileMtx,
                const uint32_t BATCH_SIZE, uint32_t &lineno,
                const std::function< void(std::string&, uint32_t) > &processLine )
{
    using namespace std;

    int     i = 0, j = 0;
    vector< string >   lines( BATCH_SIZE );
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

// 加载 users.csv
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

    // 从行文本中读入User信息并创建User
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

    // 多线程读入文件
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

// 加载 items.csv
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

// 加载 interactions_train.csv 只导入用于训练的interaction数据
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
        User *pUser;
        Item *pItem;

        stringstream str(line);
        str >> userID >> itemID >> interactType >> timestamp;
        assert( interactType < N_INTERACTION_TYPE );
        if ( !g_pUserDB->queryUser(userID, pUser) ) {
            // LOG(INFO) << "load_interaction_data cannot find user: " << userID;
            return;
        } // if
        if ( !g_pItemDB->queryItem(itemID, pItem) ) {
            // LOG(INFO) << "load_interaction_data cannot find item: " << itemID;
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
        pUser->addInteraction( pInterRec.get() );
        pItem->addInteraction( pInterRec.get() );
    }; // end processLine

    boost::thread_group thrgroup;
    for( uint32_t i = 0; i < g_nMaxThread; ++i )
        thrgroup.create_thread( std::bind(load_file_thread_routine,
                                    std::ref(inFile), std::ref(fileMtx),
                                    BATCH_SIZE, std::ref(lineno), processLine) );
    thrgroup.join_all();

    // sort users' interactions and items' interaction, by time later to earlier
/*
 *     cout << "Sorting interations by time......" << endl;
 *     auto sortInteractions = []( const InteractionRecord_wptr &pLeft,
 *                                     const InteractionRecord_wptr &pRight )->bool
 *     { return pLeft.lock()->time() > pRight.lock()->time(); };
 * 
 *     g_pUserDB->sortInteractions( sortInteractions );
 *     g_pItemDB->sortInteractions( sortInteractions );
 */
}


// 加载 interactions_test.csv 只保留正反馈记录
static
void load_test_data( const char *filename )
{
    using namespace std;

    ifstream inFile( filename, ios::in );

    if( !inFile )
        throw runtime_error( "Cannot open iteraction data file!" );

    // skip the title line
    string line;
    getline( inFile, line );
    if( !inFile )
        throw runtime_error( "Invalid interaction data format!" );

    uint32_t userID, itemID, type;
    while (getline(inFile, line)) {
        stringstream str(line);
        str >> userID >> itemID >> type;
        if (type == DELETE)
            continue;
        g_TestData[userID].insert(itemID);
    } // while
}

/**
 * @brief 对单个用户推荐结果评分，评分方法见官方说明文档
 *
 * @param rcmdItems        推荐物品集合
 * @param relevantItems    标准答案
 * @param nCorrect         推荐正确的数目
 * @return                 得分
 */
static
float score_one( const std::vector<uint32_t> &rcmdItems, 
                 const std::set<uint32_t> &relevantItems,
                 uint32_t &nCorrect,
                 float &precision2, float &precision4, float &precision6,
                 float &precision20, float &precision30, float &fRecall )
{
    using namespace std;

    bool              userSuccess = false;

    auto prescisionAtk = [&] (uint32_t k)->float {
        auto endIt = (k < rcmdItems.size() 
                        ? rcmdItems.begin() + k
                        : rcmdItems.end());

        vector<uint32_t> interSet;
        std::set_intersection( rcmdItems.begin(), endIt, 
                               relevantItems.begin(), relevantItems.end(),
                               std::back_inserter(interSet) );

        return interSet.size() / (float)k;
    };

    auto recall = [&] ()->float {
        auto endIt = (RECALL_SIZE < rcmdItems.size() 
                        ? rcmdItems.begin() + RECALL_SIZE
                        : rcmdItems.end());

        vector<uint32_t> interSet;
        std::set_intersection( rcmdItems.begin(), endIt, 
                               relevantItems.begin(), relevantItems.end(),
                               std::back_inserter(interSet) );

        nCorrect = interSet.size();
        userSuccess = (interSet.empty() ? false : true);
        precision30 = (float)(interSet.size()) / RECALL_SIZE;

        return (float)(interSet.size()) / relevantItems.size();
    };

    precision2 = prescisionAtk(2);
    precision4 = prescisionAtk(4);
    precision6 = prescisionAtk(6);
    precision20 = prescisionAtk(20);
    fRecall = recall();

    return (20 * (precision2 + precision4 + fRecall + userSuccess) + 
            10 * (precision6 + precision20));
}

static
void recommend_with_UserCF_OpenMP( uint32_t k, const char *filename )
{
    using namespace std;

    float score = 0.0;

    ofstream ofs(filename, ios::out);
    if (!ofs) {
        cerr << "Cannot open " << filename << " for writting!" << endl;
        return;
    } // if

    ofs << "UserID\tN_Correct\tPrecisionAt2\tPrecisionAt4\tPrecisionAt6\tPrecisionAt20\tPrecisionAt30\tRecall\tRecommendedItems" << endl;

    auto process = [&]( const TestDataSet::value_type &v ) {
        uint32_t                 uID = v.first;
        const std::set<uint32_t> &testItemSet = v.second;
        User                     *pUser = NULL;

        if ( !g_pUserDB->queryUser(uID, pUser) ) {
            LOG(INFO) << "No user " << uID << " found in user database.";
            return;
        } // if

        std::vector<RcmdItem> rcmdItems;
        UserCF( pUser, k, RECALL_SIZE, rcmdItems );
        if (rcmdItems.empty()) {
            LOG(INFO) << "No item recommended to user " << uID;
            return;
        } // if

        std::vector<uint32_t> rItemIds( rcmdItems.size() );
        for (std::size_t i = 0; i != rcmdItems.size(); ++i)
            rItemIds[i] = rcmdItems[i].pItem->ID();
        uint32_t nCorrect;
        float precision2, precision4, precision6, precision20, precision30, fRecall;
        float localScore = score_one( rItemIds, testItemSet, nCorrect,
                precision2, precision4, precision6, precision20, precision30, fRecall );
#pragma omp critical
        {
            score += localScore;
            ofs << std::setprecision(3) << uID << "\t" << nCorrect << "\t" 
                               << precision2 << "\t" << precision4 << "\t" 
                               << precision6 << "\t" << precision20 << "\t"
                               << precision30 << "\t" << fRecall << "\t";
            for (auto rit = rcmdItems.begin(); rit != rcmdItems.end()-1; ++rit)
                ofs << rit->pItem->ID() << ":" << rit->weight << ",";
            ofs << rcmdItems.back().pItem->ID() << ":" << rcmdItems.back().weight << endl;
        } // omp critical
    };

    // TODO cannot run on mac, use pointers instead
#pragma omp parallel
#pragma omp single
    {
    for (auto it = g_TestData.begin(); it != g_TestData.end(); ++it)
#pragma omp task firstprivate(it)
        process(*it);
#pragma omp taskwait
    } // omp single

    cout << "Total score: " << score << endl;
}

/**
 * @brief UserCF 多线程版
 *
 * @param k         查找相似物品个数上限
 * @param filename  结果写入文件
 */
static
void recommend_with_UserCF_mt( uint32_t k, const char *filename )
{
    using namespace std;

    float         score = 0.0;
    auto          it = g_TestData.begin();
    boost::mutex  itMtx, scoreMtx, fileMtx;

    ofstream ofs(filename, ios::out);
    if (!ofs) {
        cerr << "Cannot open " << filename << " for writting!" << endl;
        return;
    } // if

    // 结果文件标题
    ofs << "UserID\tN_Correct\tPrecisionAt2\tPrecisionAt4\tPrecisionAt6\tPrecisionAt20\tPrecisionAt30\tRecall\tRecommendedItems" << endl;

    // 每一个线程从测试数据集中取数据，调用UserCF算法，进行结果评分，写入文件
    auto threadRoutine = [&] {
        while (true) {
            boost::unique_lock< boost::mutex >  itlck(itMtx);
            if (it == g_TestData.end())
                return;
            uint32_t                 uID = it->first;
            const std::set<uint32_t> &testItemSet = it->second;
            ++it;
            itlck.unlock();

            User                *pUser = NULL;
            if ( !g_pUserDB->queryUser(uID, pUser) ) {
                LOG(INFO) << "No user " << uID << " found in user database.";
                continue;
            } // if

            std::vector<RcmdItem> rcmdItems;
            UserCF( pUser, k, RECALL_SIZE, rcmdItems );
            if (rcmdItems.empty()) {
                LOG(INFO) << "No item recommended to user " << uID;
                continue;
            } // if

            std::vector<uint32_t> rItemIds( rcmdItems.size() );
            for (std::size_t i = 0; i != rcmdItems.size(); ++i)
                rItemIds[i] = rcmdItems[i].pItem->ID();

            uint32_t nCorrect;
            float precision2, precision4, precision6, precision20, precision30, fRecall;
            float localScore = score_one( rItemIds, testItemSet, nCorrect,
                        precision2, precision4, precision6, precision20, precision30, fRecall );

            boost::unique_lock< boost::mutex >  scLck(scoreMtx);
            score += localScore;
            scLck.unlock();

            // 写入结果到文件
            boost::unique_lock< boost::mutex >  fLck(fileMtx);
            ofs << std::setprecision(3) << uID << "\t" << nCorrect << "\t" 
                               << precision2 << "\t" << precision4 << "\t" 
                               << precision6 << "\t" << precision20 << "\t"
                               << precision30 << "\t" << fRecall << "\t";
            for (auto rit = rcmdItems.begin(); rit != rcmdItems.end()-1; ++rit)
                ofs << rit->pItem->ID() << ":" << rit->weight << ",";
            ofs << rcmdItems.back().pItem->ID() << ":" << rcmdItems.back().weight << endl;
            fLck.unlock();
        } // while
    };

    boost::thread_group thrgroup;
    for( uint32_t i = 0; i < g_nMaxThread; ++i )
        thrgroup.create_thread( threadRoutine );
    thrgroup.join_all();

    cout << "Total score: " << score << endl;
}

static
void recommend_with_ItemCF_OpenMP( uint32_t k, const char *filename )
{
    using namespace std;

    float score = 0.0;

    ofstream ofs(filename, ios::out);
    if (!ofs) {
        cerr << "Cannot open " << filename << " for writting!" << endl;
        return;
    } // if

    ofs << "UserID\tN_Correct\tPrecisionAt2\tPrecisionAt4\tPrecisionAt6\tPrecisionAt20\tPrecisionAt30\tRecall\tRecommendedItems" << endl;

    auto process = [&]( const TestDataSet::value_type &v ) {
        uint32_t                 uID = v.first;
        const std::set<uint32_t> &testItemSet = v.second;
        User                     *pUser = NULL;

        if ( !g_pUserDB->queryUser(uID, pUser) ) {
            LOG(INFO) << "No user " << uID << " found in user database.";
            return;
        } // if

        std::vector<RcmdItem> rcmdItems;
        ItemCF( pUser, k, RECALL_SIZE, rcmdItems );
        if (rcmdItems.empty()) {
            LOG(INFO) << "No item recommended to user " << uID;
            return;
        } // if

        std::vector<uint32_t> rItemIds( rcmdItems.size() );
        for (std::size_t i = 0; i != rcmdItems.size(); ++i)
            rItemIds[i] = rcmdItems[i].pItem->ID();
        uint32_t nCorrect;
        float precision2, precision4, precision6, precision20, precision30, fRecall;
        float localScore = score_one( rItemIds, testItemSet, nCorrect,
                precision2, precision4, precision6, precision20, precision30, fRecall );
#pragma omp critical
        {
            score += localScore;
            ofs << std::setprecision(3) << uID << "\t" << nCorrect << "\t" 
                               << precision2 << "\t" << precision4 << "\t" 
                               << precision6 << "\t" << precision20 << "\t"
                               << precision30 << "\t" << fRecall << "\t";
            for (auto rit = rcmdItems.begin(); rit != rcmdItems.end()-1; ++rit)
                ofs << rit->pItem->ID() << ":" << rit->weight << ",";
            ofs << rcmdItems.back().pItem->ID() << ":" << rcmdItems.back().weight << endl;
        } // omp critical
    };

    cout << "Getting all items similarities..." << endl;
    get_all_items_similarity( k );
    cout << "Getting all items similarities done!" << endl;

#pragma omp parallel
#pragma omp single
    {
    for (auto it = g_TestData.begin(); it != g_TestData.end(); ++it)
#pragma omp task firstprivate(it)
        process(*it);
#pragma omp taskwait
    } // omp single

    cout << "Total score: " << score << endl;
}

// 过程同 recommend_with_UserCF_mt
static
void recommend_with_ItemCF_mt( uint32_t k, const char *filename )
{
    using namespace std;

    float score = 0.0;

    ofstream ofs(filename, ios::out);
    if (!ofs) {
        cerr << "Cannot open " << filename << " for writting!" << endl;
        return;
    } // if

    ofs << "UserID\tN_Correct\tPrecisionAt2\tPrecisionAt4\tPrecisionAt6\tPrecisionAt20\tPrecisionAt30\tRecall\tRecommendedItems" << endl;

    boost::mutex mtx;

    auto process = [&]( const TestDataSet::value_type &v ) {
        uint32_t                 uID = v.first;
        const std::set<uint32_t> &testItemSet = v.second;
        User                     *pUser = NULL;

        if ( !g_pUserDB->queryUser(uID, pUser) ) {
            LOG(INFO) << "No user " << uID << " found in user database.";
            return;
        } // if

        std::vector<RcmdItem> rcmdItems;
        ItemCF( pUser, k, RECALL_SIZE, rcmdItems );
        if (rcmdItems.empty()) {
            LOG(INFO) << "No item recommended to user " << uID;
            return;
        } // if

        std::vector<uint32_t> rItemIds( rcmdItems.size() );
        for (std::size_t i = 0; i != rcmdItems.size(); ++i)
            rItemIds[i] = rcmdItems[i].pItem->ID();
        uint32_t nCorrect;
        float precision2, precision4, precision6, precision20, precision30, fRecall;
        float localScore = score_one( rItemIds, testItemSet, nCorrect,
                precision2, precision4, precision6, precision20, precision30, fRecall );
        // critical section
        {
            boost::unique_lock<boost::mutex> lock(mtx);
            score += localScore;
            ofs << std::setprecision(3) << uID << "\t" << nCorrect << "\t" 
                               << precision2 << "\t" << precision4 << "\t" 
                               << precision6 << "\t" << precision20 << "\t"
                               << precision30 << "\t" << fRecall << "\t";
            for (auto rit = rcmdItems.begin(); rit != rcmdItems.end()-1; ++rit)
                ofs << rit->pItem->ID() << ":" << rit->weight << ",";
            ofs << rcmdItems.back().pItem->ID() << ":" << rcmdItems.back().weight << endl;
        } // critical section
    };

    cout << "Getting all items similarities..." << endl;
    get_all_items_similarity( k );
    cout << "Getting all items similarities done!" << endl;

    ThreadPool<std::function<void(void)>> thrpool(g_nMaxThread);
    for (auto it = g_TestData.begin(); it != g_TestData.end(); ++it)
        thrpool.addJob( std::bind(process, *it) );

    cout << "Total score: " << score << endl;
}

static
void init()
{
    g_pUserDB.reset( new UserDB );
    g_pItemDB.reset( new ItemDB );
    g_InteractStore.reset( new InteractionStore(1000) );

    g_nMaxUserID = 0;
    g_nMaxItemID = 0;

    // 得到cpu核数，最大支持同时并发线程数量
    g_nMaxThread = boost::thread::hardware_concurrency();
    if( !g_nMaxThread )
        g_nMaxThread = 1;
}

// 按需求生成指定属性的数据集，类似连接查询，与业务无关
static
void gen_join_data( const char *filename )
{
    using namespace std;

    auto cmp = []( const InteractionRecord_sptr &lhs, const InteractionRecord_sptr &rhs )->bool {
        return lhs->time() > rhs->time();
    };
    
    cout << "Sorting interactions..." << endl;
    size_t nAllInteractions = g_InteractStore->size();
    vector<InteractionRecord_sptr>      interacts;
    interacts.reserve( nAllInteractions );

    auto &content = g_InteractStore->content();
    for (uint32_t i = 0; i != InteractionStore::HASH_SIZE; ++i)
        interacts.insert( interacts.end(), content[i].begin(), content[i].end() );
    sort( interacts.begin(), interacts.end(), cmp );

    cout << "All " << interacts.size() << " interaction records." << endl;

    ofstream ofs( filename, ios::out );
    // ofs << "user_id\tuser_career_level\titem_id\titem_career_level\tinteraction_type\tcreated_at" << endl;
    ofs << "user_id\titem_id\tinteraction_type\tuser_career_level"
        << "\titem_career_level\tuser_discipline_id\titem_discipline_id"
        << "\tuser_industry_id\titem_industry_id\tuser_country"
        << "\titem_country\tuser_region\titem_region\tcreated_at" << endl;
    for ( const auto &p : interacts ) {
        User* pUser = p->user();
        Item* pItem = p->item();
        ofs << pUser->ID() << "\t" << pItem->ID() << "\t" << p->type()
            << "\t" << pUser->careerLevel() << "\t" << pItem->careerLevel()
            << "\t" << pUser->discplineID() << "\t" << pItem->discplineID()
            << "\t" << pUser->industryID() << "\t" << pItem->industryID()
            << "\t" << pUser->country() << "\t" << pItem->country()
            << "\t" << pUser->region() << "\t" << pItem->region()
            << "\t" << p->time() << endl;
        // ofs << pUser->ID() << "\t" << pUser->careerLevel() << "\t";
        // ofs << pItem->ID() << "\t" << pItem->careerLevel() << "\t";
        // ofs << p->type() << "\t" << p->time() << endl;
    } // for

    cout << "gen_join_data done!" << endl;
    exit(0);
}


int main( int argc, char **argv )
{
    using namespace std;

    google::InitGoogleLogging(argv[0]);

    try {
        // test();
        init();

        cout << "Loading users data..." << endl;
        load_user_data( "data/users.csv" );
        cout << "Loading items data..." << endl;
        load_item_data( "data/items.csv" );

        cout << "Loading interaction data..." << endl;
        load_interaction_data( "data/interactions_train.csv" );
        print_data_info();
        // gen_join_data( "data/join.csv" );
        cout << "Loading test data..." << endl;
        load_test_data( "data/interactions_test.csv" );
        cout << g_TestData.size() << " users for test." << endl;
        // gen_small_dataset( 80000, 100000 );
        // handle_command();

        // 在这里调用具体算法
        cout << "Input k for usercf / itemcf:" << endl;
        int k;
        cin >> k;
        cout << "Processing recommendation..." << endl;
        time_t now = time(0);
        cout << ctime(&now) << endl;
        // recommend_with_UserCF_OpenMP( k, "rcmd_result.txt" );
        recommend_with_UserCF_mt( k, "rcmd_result.txt" );
        // recommend_with_ItemCF_mt( 30, "rcmd_result.txt" );
        cout << "Recommendation Done!" << endl;
        now = time(0);
        cout << ctime(&now) << endl;

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

    User *pUser = NULL;
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

    Item *pItem = NULL;
    bool result = g_pItemDB->queryItem( id, pItem );
    if( result )
        cout << *pItem << endl;
    else
        cout << "No item found for id: " << id << endl;
}

/*
 * static
 * void evaluate_one( uint32_t userID, const std::vector<RcmdItem> &rcmdItems )
 * {
 *     using namespace std;
 * 
 *     auto it = g_TestData.find(userID);
 *     if (it == g_TestData.end()) {
 *         cout << "No record of user " << userID << " in test data set." << endl;
 *         return;
 *     } // if
 * 
 *     const std::set<uint32_t> &idSet = it->second;
 * 
 *     vector<uint32_t> interSet;
 *     vector<uint32_t> itemIds( rcmdItems.size() );
 * 
 *     for (size_t i = 0; i != rcmdItems.size(); ++i)
 *         itemIds[i] = rcmdItems[i].pItem->ID();
 * 
 *     sort( itemIds.begin(), itemIds.end() );
 * 
 * 
 *     std::set_intersection( idSet.begin(), idSet.end(), itemIds.begin(), itemIds.end(),
 *                            std::back_inserter(interSet) );
 * 
 *     cout << "Total " << interSet.size() << " correct recommendation." << endl;
 *     cout << "Score is " << score_one(itemIds, idSet) << endl;
 * }
 */


static
void handle_command()
{
    using namespace std;

    string      line;
    uint32_t    id;
    string      cmd;

    cout << "Please input command:" << endl;

    while ( getline(cin, line) ) {
        stringstream str(line);
        str >> cmd >> id;
        if ( "quit" == cmd )
            break;
        else if ( "user" == cmd )
            print_user_info( id );
        else if ( "item" == cmd )
            print_item_info( id );
        else if ( "usercf" == cmd ) {
            int k, nItems;
            str >> k >> nItems;
            User *pUser = NULL;
            if ( !g_pUserDB->queryUser(id, pUser) ) {
                cout << "Cannot find user " << id << endl;
                continue;
            } // if
            std::vector<RcmdItem> rcmdItems;
            UserCF( pUser, k, nItems, rcmdItems );
            cout << "Total " << rcmdItems.size() << " recommended items." << endl;
            for (const auto &i : rcmdItems)
                cout << i.pItem->ID() << "\t" << i.weight << endl;
            // evaluate_one( id, rcmdItems );
        } else {
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

/*
 * static
 * void gen_small_dataset( uint32_t nUsers, uint32_t nItems = 0 )
 * {
 *     using namespace std;
 * 
 *     const uint32_t MAXUID = g_nMaxUserID + 1;
 *     const uint32_t MAXIID = g_nMaxItemID + 1; 
 *     UIntSet users, items;
 *     User_sptr pUser;
 * 
 *     cout << "Generating small dataset......" << endl;
 * 
 *     // 对 interaction 排序，按时间由近及远
 *     auto cmpInteractByTime = []( const InteractionRecord_wptr pLeft, 
 *                                 const InteractionRecord_wptr pRight )->bool
 *     { return pLeft.lock()->time() > pRight.lock()->time(); };
 * 
 *     std::multimap< uint32_t, InteractionRecord_wptr > interactions;
 * 
 *     auto createDataFile = []( const char *inFileName, const char *outFileName, const UIntSet &set ) {
 *         uint32_t id;
 *         string line;
 * 
 *         ifstream inFile( inFileName, ios::in );
 *         ofstream outFile( outFileName, ios::out );
 *         
 *         // copy the title line
 *         getline( inFile, line );
 *         outFile << line << endl;
 * 
 *         while (getline(inFile, line)) {
 *             if (sscanf( line.c_str(), "%u", &id ) != 1)
 *                 continue;
 *             if (set.find(id) != set.end())
 *                 outFile << line << endl;
 *         } // while
 *     };
 * 
 *     auto createInteractFile = [&]( const char *filename ) {
 *         // std::vector< InteractionRecord_wptr > arr( interactions.begin(), interactions.end() );
 *         std::vector< InteractionRecord_wptr > arr;
 *         arr.reserve( interactions.size() );
 *         for( const auto &v : interactions )
 *             arr.push_back( v.second );
 * 
 *         std::sort( arr.begin(), arr.end(), cmpInteractByTime );
 * 
 *         ofstream outFile( filename, ios::out );
 *         outFile << "user_id\titem_id\tinteraction_type\tcreated_at" << endl;
 * 
 *         for( const auto &v : arr ) {
 *             auto sp = v.lock();
 *             outFile << sp->user().lock()->ID() << "\t" << sp->item().lock()->ID() << "\t"
 *                 << sp->type() << "\t" << sp->time() << endl;
 *         } // for
 *     };
 * 
 *     // 默认 item 小数据集是选取的 users 交互过的所有item
 *     auto dumpInteractedItemIDs = [&] {
 *         InteractionTable &iTable = pUser->interactionTable();        
 *         for( uint32_t i = 1; i < N_INTERACTION_TYPE; ++i ) {
 *             for( uint32_t j = 0; j < iTable[i].size(); ++j ) {
 *                 uint32_t itemID = iTable[i][j].lock()->item().lock()->ID();
 *                 items.insert( itemID );
 *                 interactions.insert( std::make_pair( itemID, iTable[i][j] ) );
 *             } // for j
 *         } // for i
 *     };
 * 
 *     // 随机选取指定用户数，至少有一个 interaction 记录
 *     srand( time(0) );
 *     while( users.size() < nUsers ) {
 *         if( g_pUserDB->queryUser(rand() % MAXUID, pUser) && pUser->nInteractions() ) {
 *             users.insert( pUser->ID() );
 *             dumpInteractedItemIDs();
 *         } // if
 *     } // while
 * 
 *     createDataFile( "users.csv", "users_small.csv", users );
 *     createDataFile( "items.csv", "items_small1.csv", items );
 *     createInteractFile( "interactions_small1.csv" );
 * 
 *     // 按照上面的方法生成的 items 小数据集可能比需要的多，随机删除，直到指定数目。
 *     if( nItems ) {
 *         std::vector<uint32_t> arr( items.begin(), items.end() );
 *         while( arr.size() > nItems ) {
 *             uint32_t index = rand() % arr.size();
 *             std::vector<uint32_t>::iterator it = arr.begin() + index;
 *             interactions.erase( *it );
 *             arr.erase( it );
 *         } // while
 *         std::set<uint32_t> tmp(arr.begin(), arr.end());
 *         items.swap( tmp );
 *         createDataFile( "items.csv", "items_small2.csv", items );
 *         createInteractFile( "interactions_small2.csv" );
 *     } // if
 * 
 *     return;
 * }
 */




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





