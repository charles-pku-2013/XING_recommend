#ifndef _COMMON_H_
#define _COMMON_H_

#include <cstdint>
#include <string>
#include <cstring>
#include <memory>
#include <ctime>
#include <set>
#include <map>
#include <vector>
#include <sstream>
#include <iterator>
#include <algorithm>
#include <functional>

class Item;
class User;
class InteractionRecord;

typedef std::shared_ptr<Item>       Item_sptr;
typedef std::shared_ptr<const Item> Item_csptr;
typedef std::weak_ptr<Item>         Item_wptr;
typedef std::weak_ptr<const Item>   Item_cwptr;
typedef std::shared_ptr<User>       User_sptr;
typedef std::shared_ptr<const User> User_csptr;
typedef std::weak_ptr<User>         User_wptr;
typedef std::weak_ptr<const User>   User_cwptr;


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


class InteractionRecord {
public:
    InteractionRecord() {}
    InteractionRecord( User_wptr pUser, Item_wptr pItem,
                    uint32_t type, const time_t &ts )
        : m_pUser(pUser), m_pItem(pItem), m_nType(type), m_tTime(ts)
    {}

    User_wptr user()
    { return m_pUser; }
    User_cwptr user() const
    { return m_pUser; }
    uint32_t userID() const;

    Item_wptr item()
    { return m_pItem; }
    Item_cwptr item() const
    { return m_pItem; }
    uint32_t itemID() const;

    uint32_t& type()
    { return m_nType; }
    uint32_t type() const
    { return m_nType; }

    time_t& time()
    { return m_tTime; }
    const time_t& time() const
    { return m_tTime; }

private:
    User_wptr       m_pUser;
    Item_wptr       m_pItem;
    uint32_t        m_nType;
    time_t          m_tTime;
};

typedef std::shared_ptr< InteractionRecord >       InteractionRecord_sptr;
typedef std::shared_ptr< const InteractionRecord > InteractionRecord_csptr;
typedef std::weak_ptr< InteractionRecord >         InteractionRecord_wptr;
typedef std::weak_ptr< const InteractionRecord >   InteractionRecord_cwptr;
typedef std::function<bool(const InteractionRecord_wptr&, const InteractionRecord_wptr&)>
            InteractionRecordCmpFunc;    // for sorting interactions
typedef std::vector< InteractionRecord_sptr >      InteractArray; // only used to define global var
extern InteractArray                               g_InteractRecords;
// below used by User and Item
typedef std::vector< InteractionRecord_wptr >      InteractionVector;
typedef InteractionVector    InteractionTable[ N_INTERACTION_TYPE ];


class User {
public:
    enum EDU_DEGREE {
        UNKNOWN,
        BACHELOR,
        MASTER,
        PHD,
        N_EDU_DEGREE
    };

public:
    User() : m_ID(0), m_nCareerLevel(0), m_DiscplineID(0), m_IndustryID(0)
           , m_nRegion(0), m_nExperienceEntries(0), m_nExperienceYears(0)
           , m_nExperienceYearsCurrent(0), m_nEduDegree(0)
    {}

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

    void addInteraction( const InteractionRecord_sptr &p )
    {  m_InteractionTable[ p->type() ].push_back(p); }
    InteractionTable& interactionTable()
    { return m_InteractionTable; }
    const InteractionTable& interactionTable() const
    { return m_InteractionTable; }
    InteractionVector& interactionVector( uint32_t type_index )
    { return m_InteractionTable[ type_index ]; }
    const InteractionVector& interactionVector( uint32_t type_index ) const
    { return m_InteractionTable[ type_index ]; }
    void sortInteractions( const InteractionRecordCmpFunc &cmp );

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
    InteractionTable        m_InteractionTable;
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

public:
    Item() : m_ID(0), m_nCareerLevel(0), m_DiscplineID(0), m_IndustryID(0)
           , m_nRegion(0), m_fLatitude(0.0), m_fLongitude(0.0)
           , m_nEmploymentType(0), m_tCreateTime(0), m_bActive(false)
    {}

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

    void addInteraction( const InteractionRecord_sptr &p )
    {  m_InteractionTable[ p->type() ].push_back(p); }
    InteractionTable& interactionTable()
    { return m_InteractionTable; }
    const InteractionTable& interactionTable() const
    { return m_InteractionTable; }
    InteractionVector& interactionVector( uint32_t type_index )
    { return m_InteractionTable[ type_index ]; }
    const InteractionVector& interactionVector( uint32_t type_index ) const
    { return m_InteractionTable[ type_index ]; }
    void sortInteractions( const InteractionRecordCmpFunc &cmp );

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
    InteractionTable        m_InteractionTable;
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

    bool queryUser( uint32_t id, User_sptr &pRet )
    {
        UserDBRecord &rec = m_UserDB[ id % HASH_SIZE ];
        auto it = rec.find( id );
        if( it != rec.end() ) {
            pRet = it->second;
            return true;
        } // if
        return false;
    }

    uint32_t size() const
    {
        uint32_t totalSize = 0;
        for( uint32_t i = 0; i < HASH_SIZE; ++i )
            totalSize += m_UserDB[i].size();
        return totalSize;
    }

    void sortInteractions( const InteractionRecordCmpFunc &cmp )
    {
        for( uint32_t i = 0; i < HASH_SIZE; ++i ) {
            for( auto it = m_UserDB[i].begin(); it != m_UserDB[i].end(); ++it )
                it->second->sortInteractions( cmp );
        } // for
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

    bool queryItem( uint32_t id, Item_sptr &pRet )
    {
        ItemDBRecord &rec = m_ItemDB[ id % HASH_SIZE ];
        auto it = rec.find( id );
        if( it != rec.end() ) {
            pRet = it->second;
            return true;
        } // if
        return false;
    }

    uint32_t size() const
    {
        uint32_t totalSize = 0;
        for( uint32_t i = 0; i < HASH_SIZE; ++i )
            totalSize += m_ItemDB[i].size();
        return totalSize;
    }

    void sortInteractions( const InteractionRecordCmpFunc &cmp )
    {
        for( uint32_t i = 0; i < HASH_SIZE; ++i ) {
            for( auto it = m_ItemDB[i].begin(); it != m_ItemDB[i].end(); ++it )
                it->second->sortInteractions( cmp );
        } // for
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
