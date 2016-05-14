#include "common.h"
#include <glog/logging.h>


FAST_ALLOCATOR( InteractionRecord )  InteractionRecord::s_allocator;
FAST_ALLOCATOR( User )  User::s_allocator;
FAST_ALLOCATOR( Item )  Item::s_allocator;


/*
 * const char *CAREER_LEVEL_TEXT[] = {
 *     "Unknown",
 *     "Student/Intern",
 *     "Entry Level",
 *     "Professional/Experienced",
 *     "Manager",
 *     "Executive",
 *     "Senior Executive"
 * };
 *
 * const char *EDU_DEGREE_TEXT[] = {
 *     "Unknown",
 *     "Bachelor",
 *     "Master",
 *     "PhD"
 * };
 */

bool ItemPtrCmp::operator() (const Item *lhs, 
                             const Item *rhs) const
{ return lhs->ID() < rhs->ID(); };

bool UserPtrCmp::operator() (const User *lhs, 
                             const User *rhs) const
{ return lhs->ID() < rhs->ID(); };

uint32_t InteractionRecord::userID() const
{ return user()->ID(); }

uint32_t InteractionRecord::itemID() const
{ return item()->ID(); }

/*
 * void User::sortInteractions( const InteractionRecordCmpFunc &cmp )
 * {
 *     for( uint32_t i = 1; i < N_INTERACTION_TYPE; ++i )
 *         sort( m_InteractionTable[i].begin(), m_InteractionTable[i].end(), cmp );
 * }
 *
 * void Item::sortInteractions( const InteractionRecordCmpFunc &cmp )
 * {
 *     for( uint32_t i = 1; i < N_INTERACTION_TYPE; ++i )
 *         sort( m_InteractionTable[i].begin(), m_InteractionTable[i].end(), cmp );
 * }
 */

void User::addInteraction( InteractionRecord *p )
{
    Item *pItem = p->item();
    uint32_t  itemID = pItem->ID();
    InteractionMap& _map = m_InteractionTable[ p->type() ];
    boost::unique_lock< InteractionMap > lock(_map);
    InteractionVector &vec = _map[ itemID ];
    vec.push_back(p);
}

ItemSet& User::interestedItemSet( bool update )
{
    //!! double check
    if (m_setInterestedItemPtrs.empty() || update) {
        boost::unique_lock<User> lock(*this);
        if (m_setInterestedItemPtrs.empty() || update)
            updateInterest();
    } // if
    return m_setInterestedItemPtrs;
}

std::set<uint32_t>& User::interestedItemIdSet( bool update )
{
    if (m_setInterestedItemIds.empty() || update) {
        boost::unique_lock<User> lock(*this);
        if (m_setInterestedItemIds.empty() || update)    
            updateInterest();
    } // if
    return m_setInterestedItemIds;
}

void User::updateInterest()
{
    m_setInterestedItemPtrs.clear();
    m_setInterestedItemIds.clear();

    for (uint32_t i = CLICK; i != DELETE; ++i) {
        InteractionMap &iMap = this->interactionMap( i );
        if (iMap.empty())
            continue;
        for (auto it = iMap.begin(); it != iMap.end(); ++it) {
            InteractionVector &vec = it->second;
            // assert( !vec.empty() );
            InteractionRecord *pInt = vec[0];
            Item* pItem = pInt->item();
            m_setInterestedItemPtrs.insert( pItem );
            m_setInterestedItemIds.insert( pItem->ID() );
        } // for
    } // for
}

UserSet& Item::interestedUserSet( bool update )
{
    if (m_setInterestedUserPtrs.empty() || update) {
        boost::unique_lock<Item> lock(*this);
        if (m_setInterestedUserPtrs.empty() || update)
            updateInterest();
    } // if
    return m_setInterestedUserPtrs;
}

std::set<uint32_t>& Item::interestedUserIdSet( bool update )
{
    if (m_setInterestedUserIds.empty() || update) {
        boost::unique_lock<Item> lock(*this);
        if (m_setInterestedUserIds.empty() || update)
            updateInterest();
    } // if
    return m_setInterestedUserIds;
}

void Item::updateInterest()
{
    m_setInterestedUserPtrs.clear();
    m_setInterestedUserIds.clear();

    for (uint32_t i = CLICK; i != DELETE; ++i) {
        InteractionMap &iMap = this->interactionMap( i );
        if (iMap.empty())
            continue;
        for (auto it = iMap.begin(); it != iMap.end(); ++it) {
            InteractionVector &vec = it->second;
            // assert( !vec.empty() );
            InteractionRecord *pInt = vec[0];
            User *pUser = pInt->user();
            m_setInterestedUserPtrs.insert( pUser );
            m_setInterestedUserIds.insert( pUser->ID() );
        } // for
    } // for
}

void Item::addInteraction( InteractionRecord *p )
{
    User *pUser = p->user();
    uint32_t  userID = pUser->ID();
    InteractionMap& _map = m_InteractionTable[ p->type() ];
    boost::unique_lock< InteractionMap > lock(_map);
    InteractionVector &vec = _map[ userID ];
    vec.push_back(p);
}


// user.csv 中有重复记录，如 id == 24
void UserDB::addUser( const User_sptr &pUser )
{
    uint32_t      id = pUser->ID();
    UserDBRecord& rec = m_UserDB[ id % HASH_SIZE ];
    boost::unique_lock< UserDBRecord > lock(rec);
    rec.insert( std::make_pair(id, pUser) );

    // test
    // char errstr[80];
    // sprintf( errstr, "User %u already exist.", id );
    // auto ret = m_UserDB[ id % HASH_SIZE ].insert( std::make_pair(id, pUser) );
    // if( !ret.second )
        // LOG(WARNING) << errstr;
}

/*
 * void UserDB::sortInteractionsThreadFunc( uint32_t &index, boost::mutex &mtx,
 *                                     const InteractionRecordCmpFunc &cmp )
 * {
 *     while (true) {
 *         boost::unique_lock< boost::mutex > lock(mtx);
 *         if( index >= HASH_SIZE )
 *             return;
 *
 *         UserDBRecord &rec = m_UserDB[ index++ ];
 *         lock.unlock();
 *
 *         for( auto it = rec.begin(); it != rec.end(); ++it )
 *             it->second->sortInteractions( cmp );
 *     } // while
 * }
 *
 * void UserDB::sortInteractions( const InteractionRecordCmpFunc &cmp )
 * {
 *     uint32_t index = 0;   // [0, HASH_SIZE)
 *     boost::mutex mtx;     // for accessing index
 *
 *     boost::thread_group thrgroup;
 *     for( uint32_t i = 0; i < g_nMaxThread; ++i )
 *         thrgroup.create_thread( std::bind(&UserDB::sortInteractionsThreadFunc, this,
 *                                     std::ref(index), std::ref(mtx), std::ref(cmp)) );
 *     thrgroup.join_all();
 * }
 */


void ItemDB::addItem( const Item_sptr &pItem )
{
    uint32_t      id = pItem->ID();
    ItemDBRecord& rec = m_ItemDB[ id % HASH_SIZE ];
    boost::unique_lock< ItemDBRecord >  lock(rec);
    rec.insert( std::make_pair(id, pItem) );

    // test
    // char errstr[80];
    // sprintf( errstr, "Item %u already exist.", id );
    // auto ret = m_ItemDB[ id % HASH_SIZE ].insert( std::make_pair(id, pItem) );
    // if( !ret.second )
        // LOG(WARNING) << errstr;
}

float get_factor(std::size_t n)
{
    static const uint32_t SIZE = 1000;
    static std::vector<float> values;
    static std::once_flag onceFlag;

    std::call_once(onceFlag, []{
        values.resize(SIZE + 1);
        for (std::size_t i = 1; i <= SIZE; ++i)
            values[i] = 1.0 / std::log(1.0 + i);
    });

    if (n <= SIZE)
        return values[n];

    return (1.0 / std::log(1.0 + n));
}

/*
 * void ItemDB::sortInteractionsThreadFunc( uint32_t &index, boost::mutex &mtx,
 *                                     const InteractionRecordCmpFunc &cmp )
 * {
 *     while (true) {
 *         boost::unique_lock< boost::mutex > lock(mtx);
 *         if( index >= HASH_SIZE )
 *             return;
 *
 *         ItemDBRecord &rec = m_ItemDB[ index++ ];
 *         lock.unlock();
 *
 *         for( auto it = rec.begin(); it != rec.end(); ++it )
 *             it->second->sortInteractions( cmp );
 *     } // while
 * }
 *
 * void ItemDB::sortInteractions( const InteractionRecordCmpFunc &cmp )
 * {
 *     uint32_t index = 0;   // [0, HASH_SIZE)
 *     boost::mutex mtx;     // for accessing index
 *
 *     boost::thread_group thrgroup;
 *     for( uint32_t i = 0; i < g_nMaxThread; ++i )
 *         thrgroup.create_thread( std::bind(&ItemDB::sortInteractionsThreadFunc, this,
 *                                     std::ref(index), std::ref(mtx), std::ref(cmp)) );
 *     thrgroup.join_all();
 * }
 */

