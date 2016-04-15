#include "recommend_algorithm.h"
#include <cmath>
#include <functional>
#include <algorithm>


std::size_t UserCF( const User_sptr &user, std::size_t k, std::size_t nItems, 
                    std::vector<RcmdItem> &rcmdItems )
{
    using namespace std;

    typedef std::map<User_sptr, float, UserPtrCmp>  UserSimMap;

    // first, find all items that "user" has positive interactions.
    // 找出目标用户u所有的兴趣物品集合N(u).
    ItemSet     setNu;
    user->interestedItems( setNu );

    // test
    /*
     * for (const auto &i : setNu)
     *     cout << i->ID() << " ";
     * cout << endl;
     * cout << "size of setNu is: " << setNu.size() << endl;
     */

    // 用于计算用户 u, v 的相似度, user 到 userV 的相似度
    UserSimMap wuv;

    // 对N(u)中的每一个物品 i∈N(u), 找出i的兴趣用户集合N(i)
    for (const Item_sptr &itemI : setNu) {
        UserSet setNi;
        itemI->interestedByUsers( setNi );
        for (const User_sptr &userV : setNi) {
            if (userV->ID() == user->ID())
                continue;
            wuv[userV] += 1.0 / std::log(1.0 + setNi.size());
        } // for v
    } // for i

    // 利用上一步结果计算用户u和v相似度 wuv.
    for (auto &v : wuv) {
        ItemSet                   setNv;
        v.first->interestedItems( setNv );
        v.second /= std::sqrt( (float)(setNu.size()) * setNv.size() );
    } // for

    // 从和目标用户u有相似度的用户集合v∈N(i)中找出前K个最相似的用户S(u,K)
    //!! 不可以直接用 map::value_type, 其pair.first是const
    typedef std::pair< User_sptr, float > UserSimPair;
    std::vector<UserSimPair> userSimValue( wuv.begin(), wuv.end() );
    std::vector<UserSimPair>::iterator endIt;

    auto userSimValueCmp = [] ( const UserSimPair &lhs,
                             const UserSimPair &rhs )->bool
                        { return lhs.second > rhs.second; };

    if (k < userSimValue.size()) {
        std::partial_sort( userSimValue.begin(), userSimValue.begin() + k, 
                   userSimValue.end(), userSimValueCmp );
        endIt = userSimValue.begin() + k;
    } else {
        std::sort( userSimValue.begin(), userSimValue.end(), userSimValueCmp );
        endIt = userSimValue.end();
    } // if

/*
 * 对与S(u,K)中的每一个用户 v∈S(u,k)
 * 找出v的兴趣物品列表N(v)
 * 对于物品 i∈N(v), 若i不在目标用户user的兴趣物品列表中，
 * 则物品i对目标用户user的推荐程度 p(u,i) += wuv * rvi (这里rvi恒为1)
 */
    typedef std::map<Item_sptr, float, ItemPtrCmp> RcmdItemMap;
    RcmdItemMap rcmdItemMap;

    for (auto it = userSimValue.begin(); it != endIt; ++it) {
        User_sptr userV = it->first;
        ItemSet setNv;
        userV->interestedItems( setNv );
        // 求setNu与setNv的差 setNv - setNu  Nv有但Nu没有
        std::vector<Item_sptr> uvDiff;
        std::set_difference( setNv.begin(), setNv.end(),
                             setNu.begin(), setNu.end(),
                             std::back_inserter(uvDiff),
                             setNu.key_comp() );
        // insert them to rcmdItemMap
        for (auto &i : uvDiff)
            rcmdItemMap[i] += wuv[userV];
    } // for

    rcmdItems.resize( rcmdItemMap.size() );
    size_t idx = 0;
    for (auto &val : rcmdItemMap) {
        rcmdItems[idx].pItem = val.first;
        rcmdItems[idx].weight = val.second;
        ++idx;
    } // for

    // sort and resize to nItems
    if (nItems < rcmdItems.size()) {
        std::partial_sort( rcmdItems.begin(), rcmdItems.begin() + nItems,
                           rcmdItems.end() );
        rcmdItems.resize( nItems );
    } else {
        std::sort( rcmdItems.begin(), rcmdItems.end() );
    } // if

    return rcmdItems.size();
}
