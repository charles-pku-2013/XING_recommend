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

bool ItemSetCmp::operator() (const Item_sptr &lhs, 
                             const Item_sptr &rhs) const
{ return lhs->ID() < rhs->ID(); };

bool UserSetCmp::operator() (const User_sptr &lhs, 
        const User_sptr &rhs) const
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

void User::addInteraction( const InteractionRecord_sptr &p )
{
    Item_sptr pItem = p->item();
    uint32_t  itemID = pItem->ID();
    InteractionMap& _map = m_InteractionTable[ p->type() ];
    boost::unique_lock< InteractionMap > lock(_map);
    InteractionVector &vec = _map[ itemID ];
    vec.push_back(p);
}

std::size_t User::interestedItems( ItemSet &iSet )
{
    std::size_t count = 0;

    iSet.clear();
    for (uint32_t i = CLICK; i != DELETE; ++i) {
        InteractionMap &iMap = this->interactionMap( i );
        if (iMap.empty())
            continue;
        count += iMap.size();
        for (auto it = iMap.begin(); it != iMap.end(); ++it) {
            InteractionVector &vec = it->second;
            assert( !vec.empty() );
            InteractionRecord_sptr pInt( vec[0] );
            Item_sptr pItem = pInt->item();
            iSet.insert( pItem );
        } // for
    } // for

    return count;
}

std::size_t Item::interestedByUsers( UserSet &uSet )
{
    std::size_t count = 0;

    uSet.clear();
    for (uint32_t i = CLICK; i != DELETE; ++i) {
        InteractionMap &iMap = this->interactionMap( i );
        if (iMap.empty())
            continue;
        count += iMap.size();
        for (auto it = iMap.begin(); it != iMap.end(); ++it) {
            InteractionVector &vec = it->second;
            assert( !vec.empty() );
            InteractionRecord_sptr pInt( vec[0] );
            User_sptr pUser = pInt->user();
            uSet.insert( pUser );
        } // for
    } // for

    return count;
}

void Item::addInteraction( const InteractionRecord_sptr &p )
{
    User_sptr pUser = p->user();
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

