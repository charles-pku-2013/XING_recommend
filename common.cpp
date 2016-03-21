#include "common.h"
#include <glog/logging.h>


const char *CAREER_LEVEL_TEXT[] = {
    "Unknown",
    "Student/Intern",
    "Entry Level",
    "Professional/Experienced",
    "Manager",
    "Executive",
    "Senior Executive"
};

const char *EDU_DEGREE_TEXT[] = {
    "Unknown",
    "Bachelor",
    "Master",
    "PhD"
};

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
    m_UserDB[ id % HASH_SIZE ].insert( std::make_pair(id, pUser) );

    // test
    // char errstr[80];
    // sprintf( errstr, "User %u already exist.", id );
    // auto ret = m_UserDB[ id % HASH_SIZE ].insert( std::make_pair(id, pUser) );
    // if( !ret.second )
        // LOG(WARNING) << errstr;
}

void ItemDB::addItem( const Item_sptr &pItem )
{
    uint32_t id = pItem->ID();
    // m_ItemDB[ id % HASH_SIZE ].insert( std::make_pair(id, pItem) );
    
    // test
    char errstr[80];
    sprintf( errstr, "Item %u already exist.", id );
    auto ret = m_ItemDB[ id % HASH_SIZE ].insert( std::make_pair(id, pItem) );
    if( !ret.second )
        LOG(WARNING) << errstr;
}
