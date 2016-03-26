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

uint32_t InteractionRecord::userID() const
{ return user().lock()->ID(); }
uint32_t InteractionRecord::itemID() const
{ return item().lock()->ID(); }

void User::sortInteractions( const InteractionRecordCmpFunc &cmp )
{
    for( uint32_t i = 1; i < N_INTERACTION_TYPE; ++i )
        sort( m_InteractionTable[i].begin(), m_InteractionTable[i].end(), cmp );
}

void Item::sortInteractions( const InteractionRecordCmpFunc &cmp )
{
    for( uint32_t i = 1; i < N_INTERACTION_TYPE; ++i )
        sort( m_InteractionTable[i].begin(), m_InteractionTable[i].end(), cmp );
}


// user.csv 中有重复记录，如 id == 24
void UserDB::addUser( const User_sptr &pUser )
{
    uint32_t id = pUser->ID();
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

void UserDB::sortInteractionsThreadFunc( uint32_t &index, boost::mutex &mtx,
                                    const InteractionRecordCmpFunc &cmp )
{
    while (true) {
        boost::unique_lock< boost::mutex > lock(mtx);
        if( index >= HASH_SIZE )
            return;

        UserDBRecord &rec = m_UserDB[ index++ ];
        lock.unlock();

        for( auto it = rec.begin(); it != rec.end(); ++it )
            it->second->sortInteractions( cmp );
    } // while
}

void UserDB::sortInteractions( const InteractionRecordCmpFunc &cmp )
{
    uint32_t index = 0;   // [0, HASH_SIZE)
    boost::mutex mtx;     // for accessing index

    boost::thread_group thrgroup;
    for( uint32_t i = 0; i < g_nMaxThread; ++i )
        thrgroup.create_thread( std::bind(&UserDB::sortInteractionsThreadFunc, this,
                                    std::ref(index), std::ref(mtx), std::ref(cmp)) );
    thrgroup.join_all();
}


void ItemDB::addItem( const Item_sptr &pItem )
{
    uint32_t id = pItem->ID();
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

void ItemDB::sortInteractionsThreadFunc( uint32_t &index, boost::mutex &mtx,
                                    const InteractionRecordCmpFunc &cmp )
{
    while (true) {
        boost::unique_lock< boost::mutex > lock(mtx);
        if( index >= HASH_SIZE )
            return;

        ItemDBRecord &rec = m_ItemDB[ index++ ];
        lock.unlock();

        for( auto it = rec.begin(); it != rec.end(); ++it )
            it->second->sortInteractions( cmp );
    } // while
}

void ItemDB::sortInteractions( const InteractionRecordCmpFunc &cmp )
{
    uint32_t index = 0;   // [0, HASH_SIZE)
    boost::mutex mtx;     // for accessing index

    boost::thread_group thrgroup;
    for( uint32_t i = 0; i < g_nMaxThread; ++i )
        thrgroup.create_thread( std::bind(&ItemDB::sortInteractionsThreadFunc, this,
                                    std::ref(index), std::ref(mtx), std::ref(cmp)) );
    thrgroup.join_all();
}

