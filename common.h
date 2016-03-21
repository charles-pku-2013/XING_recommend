#ifndef _COMMON_H_
#define _COMMON_H_

#include <cstdint>
#include <string>
#include <cstring>
#include <set>
#include <memory>
#include <ctime>
#include <map>
#include <sstream>
#include <iterator>
#include <algorithm>

class Item;
class User;

typedef std::shared_ptr<Item>       Item_sptr;
typedef std::weak_ptr<Item>         Item_wptr;
typedef std::shared_ptr<User>       User_sptr;
typedef std::weak_ptr<User>         User_wptr;


enum CareerLevel {
    UNKNOWN,
    INTERN,
    ENTRY,
    PROFESSIONAL,
    MANAGER,
    EXECUTIVE,
    SENIOR_EXECUTIVE,
    N_CAREER_LEVEL
};

extern const char *CAREER_LEVEL_TEXT[];

enum INTERACTION_TYPE {
    INVALID,
    CLICK,
    BOOKMARK,
    REPLY,
    DELETE,
    N_INTERACTION_TYPE
};

extern const char *EDU_DEGREE_TEXT[];


class User {
public:
    enum EDU_DEGREE {
        UNKNOWN,
        BACHELOR,
        MASTER,
        PHD,
        N_EDU_DEGREE
    };

    struct InteractItemRecord {
        uint32_t        itemID;
        uint32_t        interactType;
        Item_wptr       pItem;
        time_t          timestamp;

        InteractItemRecord() {}

        InteractItemRecord( uint32_t id, uint32_t type, Item_wptr ptr, time_t ts )
                : itemID(id), interactType(type), pItem(ptr), timestamp(ts) {}
    };

    // 同一用户可能在不同的时间对同一物品有同样的动作，如多次点击
    typedef std::multimap< uint32_t, InteractItemRecord >  IteractItemMap;
    typedef IteractItemMap InteractItemDB[N_INTERACTION_TYPE];

public:
    uint32_t& ID() { return m_ID; }
    const uint32_t& ID() const { return m_ID; }

    std::set<uint32_t>& jobRoles()
    { return m_nsetJobRoles; }
    const std::set<uint32_t>& jobRoles() const
    { return m_nsetJobRoles; }
    void addJobRole( uint32_t id )
    { m_nsetJobRoles.insert(id); }
    bool hasJobRole( uint32_t id )
    { return m_nsetJobRoles.find(id) != m_nsetJobRoles.end(); }

    uint32_t& careerLevel()
    { return m_nCareerLevel; }
    const uint32_t& careerLevel() const
    { return m_nCareerLevel; }

    uint32_t& discplineID()
    { return m_DiscplineID; }
    const uint32_t& discplineID() const
    { return m_DiscplineID; }

    uint32_t& industryID()
    { return m_IndustryID; }
    const uint32_t& industryID() const
    { return m_IndustryID; }

    std::string& country()
    { return m_strCountry; }
    const std::string& country() const
    { return m_strCountry; }

    uint32_t& region()
    { return m_nRegion; }
    const uint32_t& region() const
    { return m_nRegion; }

    uint32_t& numOfCvEntry()
    { return m_nExperienceEntries; }
    const uint32_t& numOfCvEntry() const
    { return m_nExperienceEntries; }

    uint32_t& yearsOfExperience()
    { return m_nExperienceYears; }
    const uint32_t& yearsOfExperience() const
    { return m_nExperienceYears; }

    uint32_t& yearsOfCurrentJob()
    { return m_nExperienceYearsCurrent; }
    const uint32_t& yearsOfCurrentJob() const
    { return m_nExperienceYearsCurrent; }

    uint32_t& eduDegree()
    { return m_nEduDegree; }
    const uint32_t& eduDegree() const
    { return m_nEduDegree; }

    std::set<uint32_t>& eduFields()
    { return m_nsetEduFields; }
    const std::set<uint32_t>& eduFields() const
    { return m_nsetEduFields; }
    void addEduFields( uint32_t id )
    { m_nsetEduFields.insert(id); }

    InteractItemDB& interactedItems()
    { return m_InteractItemDB; }
    const InteractItemDB& interactedItems() const
    { return m_InteractItemDB; }
    void addInteractItem( Item_wptr pItem, uint32_t type, time_t ts );

private:
    uint32_t                m_ID;
    std::set<uint32_t>      m_nsetJobRoles;
    uint32_t                m_nCareerLevel;
    uint32_t                m_DiscplineID;
    uint32_t                m_IndustryID;
    std::string             m_strCountry;
    uint32_t                m_nRegion;
    uint32_t                m_nExperienceEntries;
    uint32_t                m_nExperienceYears;
    uint32_t                m_nExperienceYearsCurrent;
    uint32_t                m_nEduDegree;
    std::set<uint32_t>      m_nsetEduFields;
    InteractItemDB          m_InteractItemDB;
};


class Item {
public:
    enum EMPLOYMENT_TYPE {
        UNKNOWN,
        FULL_TIME,
        PART_TIME,
        FREELANCER,
        INTERN,
        VOLUNTARY,
        N_EMPLOYMENT_TYPE
    };

    struct InteractUserRecord {
        uint32_t    userID;
        uint32_t    interactType;
        User_wptr   pUser;
        time_t      timestamp;

        InteractUserRecord() {}
        InteractUserRecord( uint32_t id, uint32_t type, User_wptr ptr, time_t ts )
            : userID(id), interactType(type), pUser(ptr), timestamp(ts) {}
    };

    typedef std::multimap< uint32_t, InteractUserRecord >  InteractUserMap;
    typedef InteractUserMap  InteractUserDB[N_INTERACTION_TYPE];

public:
    uint32_t& ID() { return m_ID; }
    const uint32_t& ID() const { return m_ID; }

    std::set<uint32_t>& title()
    { return m_nsetTitle; }
    const std::set<uint32_t>& title() const
    { return m_nsetTitle; }
    void addTitle( uint32_t id )
    { m_nsetTitle.insert(id); }
    bool hasTitle( uint32_t id )
    { return m_nsetTitle.find(id) != m_nsetTitle.end(); }

    uint32_t& careerLevel()
    { return m_nCareerLevel; }
    const uint32_t& careerLevel() const
    { return m_nCareerLevel; }

    uint32_t& discplineID()
    { return m_DiscplineID; }
    const uint32_t& discplineID() const
    { return m_DiscplineID; }

    uint32_t& industryID()
    { return m_IndustryID; }
    const uint32_t& industryID() const
    { return m_IndustryID; }

    std::string& country()
    { return m_strCountry; }
    const std::string& country() const
    { return m_strCountry; }

    uint32_t& region()
    { return m_nRegion; }
    const uint32_t& region() const
    { return m_nRegion; }

    float& latitude()
    { return m_fLatitude; }
    const float& latitude() const
    { return m_fLatitude; }

    float& longitude()
    { return m_fLongitude; }
    const float& longitude() const
    { return m_fLongitude; }

    uint32_t& employmentType()
    { return m_nEmploymentType; }
    const uint32_t& employmentType() const
    { return m_nEmploymentType; }

    std::set<uint32_t>& tags()
    { return m_nsetTags; }
    const std::set<uint32_t>& tags() const
    { return m_nsetTags; }
    void addTag( uint32_t id )
    { m_nsetTags.insert(id); }
    bool hasTag( uint32_t id )
    { return m_nsetTags.find(id) != m_nsetTags.end(); }

    time_t& createTime()
    { return m_tCreateTime; }
    const time_t& createTime() const
    { return m_tCreateTime; }

    bool isActive() const
    { return m_bActive; }
    void setActive( bool status = true )
    { m_bActive = status; }

    InteractUserDB& interactedUsers()
    { return m_InteractUserDB; }
    const InteractUserDB& interactedUsers() const
    { return m_InteractUserDB; }
    void addInteractUser( User_wptr pUser, uint32_t type, time_t ts );

private:
    uint32_t                m_ID;
    std::set<uint32_t>      m_nsetTitle;
    uint32_t                m_nCareerLevel;
    uint32_t                m_DiscplineID;
    uint32_t                m_IndustryID;
    std::string             m_strCountry;
    uint32_t                m_nRegion;
    float                   m_fLatitude;   // 0 means NULL
    float                   m_fLongitude;
    uint32_t                m_nEmploymentType;
    std::set<uint32_t>      m_nsetTags;
    time_t                  m_tCreateTime;
    bool                    m_bActive;
    InteractUserDB          m_InteractUserDB;
};


class UserDB {
public:
    // total 15w users
    static const uint32_t HASH_SIZE = 1000;

    typedef std::map< uint32_t, User_sptr >   UserDBRecord;
    typedef UserDBRecord            UserDBStorage[HASH_SIZE];

public:
    UserDBStorage& content()
    { return m_UserDB; }
    const UserDBStorage& content() const
    { return m_UserDB; }

    void addUser( const User_sptr &pUser );

    User_wptr queryUser( uint32_t id )
    {
        UserDBRecord &rec = m_UserDB[ id % HASH_SIZE ];
        auto it = rec.find( id );
        if( it != rec.end() )
            return it->second;
        return User_sptr();
    }

    uint32_t size() const
    {
        uint32_t totalSize = 0;
        for( uint32_t i = 0; i < HASH_SIZE; ++i )
            totalSize += m_UserDB[i].size();
        return totalSize;
    }

private:
    UserDBStorage       m_UserDB;
};


class ItemDB {
public:
    // total 1358098 items
    static const uint32_t       HASH_SIZE = 1000;
    typedef std::map< uint32_t, Item_sptr >   ItemDBRecord;
    typedef ItemDBRecord        ItemDBStorage[HASH_SIZE];

public:
    ItemDBStorage& content()
    { return m_ItemDB; }
    const ItemDBStorage& content() const
    { return m_ItemDB; }

    void addItem( const Item_sptr &pItem );

    Item_wptr queryItem( uint32_t id )
    {
        ItemDBRecord &rec = m_ItemDB[ id % HASH_SIZE ];
        auto it = rec.find( id );
        if( it != rec.end() )
            return it->second;
        return Item_sptr();
    }

    uint32_t size() const
    {
        uint32_t totalSize = 0;
        for( uint32_t i = 0; i < HASH_SIZE; ++i )
            totalSize += m_ItemDB[i].size();
        return totalSize;
    }

private:
    ItemDBStorage       m_ItemDB;
};


extern std::unique_ptr< UserDB >        g_pUserDB;
extern std::unique_ptr< ItemDB >        g_pItemDB;

template < typename T >
bool read_from_string( const char *s, T &value )
{
    if( strcmp(s, "NULL") == 0 || strcmp(s, "null") == 0 ) {
        value = T();
        return true;
    } // if
    std::stringstream str(s);
    str >> value;
    bool ret = (str.good() || str.eof());
    if( !ret )
        value = T();
    return ret;
}


// for test
template < typename T >
void print_container( std::ostream &os, const T &c )
{
    typedef typename T::value_type value_type;
    std::copy( c.begin(), c.end(), std::ostream_iterator<value_type>(os, " ") );
    os << std::endl;
}

#endif

