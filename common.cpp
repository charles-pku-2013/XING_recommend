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


void User::addInteractItem( Item_wptr pItem, uint32_t type, time_t ts )
{
    auto sp = pItem.lock();
    
    if( !sp )
        throw std::runtime_error( "Object may has been destroyed." );

    // for test
    for( int i = 1; i < N_INTERACTION_TYPE; ++i ) {
        auto loc = m_InteractItemDB[i].equal_range( sp->ID() );
        if( loc.first != loc.second ) {
            for( auto it = loc.first; it != loc.second; ++it )
                LOG(INFO) << "User " << this->ID() << " interacted with "
                    << it->first << " at " << it->second.timestamp 
                    << " type is " << it->second.interactType;
        } // if
    } // for

    m_InteractItemDB[type].insert( std::make_pair(sp->ID(), 
                InteractItemRecord(sp->ID(), type, pItem, ts)) );
}


void Item::addInteractUser( User_wptr pUser, uint32_t type, time_t ts )
{
    auto sp = pUser.lock();

    if( !sp )
        throw std::runtime_error( "Object may has been destroyed." );

    m_InteractUserDB[type].insert( std::make_pair(sp->ID(), 
                InteractUserRecord(sp->ID(), type, pUser, ts)) );
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
