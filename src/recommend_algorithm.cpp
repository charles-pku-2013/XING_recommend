#include "recommend_algorithm.h"
#include <functional>
#include <algorithm>
#include <mutex>
#include <glog/logging.h>


std::size_t UserCF( User *user, std::size_t k, std::size_t nItems, 
                    std::vector<RcmdItem> &rcmdItems )
{
    using namespace std;

    auto err_ret = [](int retval, const char *msg) {
        cerr << msg << endl;
        return retval;
    };

    rcmdItems.clear();

    if (!k)
        err_ret(0, "Invalid k value!");

    typedef std::map<User*, float, UserPtrCmp>  UserSimMap;

    // first, find all items that "user" has positive interactions.
    // 找出目标用户u所有的兴趣物品集合N(u).
    ItemSet &setNu = user->interestedItemSet();
    if (!setNu.size()) {
        LOG(INFO) << "Target user " << user->ID() << " do not have histroy interests record, cannot recommend!";
        return 0;
    } // if

    // LOG(INFO) << "size of setNu is " << setNu.size();

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
    for (Item *itemI : setNu) {
        UserSet &setNi = itemI->interestedUserSet();
        // LOG(INFO) << "item " << itemI->ID() << " liked by " << setNi.size() << " users";
        for (User *userV : setNi) {
            if (userV->ID() == user->ID())
                continue;
            wuv[userV] += 1.0 / std::log(1.0 + setNi.size());
        } // for v
    } // for i

    // 利用上一步结果计算用户u和v相似度 wuv.
    for (auto &v : wuv) {
        ItemSet &setNv = v.first->interestedItemSet();
        // setNv.size() 肯定不为0
        v.second /= std::sqrt( (float)(setNu.size()) * setNv.size() );
    } // for

    // 从和目标用户u有相似度的用户集合v∈N(i)中(wuv != 0)找出前K个最相似的用户S(u,K)
    //!! 不可以直接用 map::value_type, 其pair.first是const
    typedef std::pair< User*, float > UserSimPair;
    std::vector<UserSimPair> userSimValue( wuv.begin(), wuv.end() );

    auto userSimValueCmp = [] ( const UserSimPair &lhs,
                                const UserSimPair &rhs )->bool
                        { return lhs.second > rhs.second; };

    if (k < userSimValue.size()) {
        std::partial_sort( userSimValue.begin(), userSimValue.begin() + k, 
                   userSimValue.end(), userSimValueCmp );
        userSimValue.resize(k);
    } else {
        std::sort( userSimValue.begin(), userSimValue.end(), userSimValueCmp );
    } // if

/*
 * 对与S(u,K)中的每一个用户 v∈S(u,k)
 * 找出v的兴趣物品列表N(v)
 * 对于物品 i∈N(v), 若i不在目标用户user的兴趣物品列表中，
 * 则物品i对目标用户user的推荐程度 p(u,i) += wuv * rvi (这里rvi恒为1)
 */
    typedef std::map<Item*, float, ItemPtrCmp> RcmdItemMap;
    RcmdItemMap rcmdItemMap;

    for (auto it = userSimValue.begin(); it != userSimValue.end(); ++it) {
        User *userV = it->first;
        ItemSet &setNv = userV->interestedItemSet();
        // 求setNu与setNv的差 setNv - setNu  Nv有但Nu没有
        std::vector<Item*> uvDiff;
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


std::size_t ItemCF( User *user, std::size_t k, std::size_t nItems,
                    std::vector<RcmdItem> &rcmdItems )
{
    using namespace std;

    LOG(INFO) << "Doing recommend for user: " << user->ID();

    // static std::once_flag onceFlag;

    auto err_ret = [](int retval, const char *msg) {
        cerr << msg << endl;
        return retval;
    };

    rcmdItems.clear();

    if (!k)
        err_ret(0, "Invalid k value!");

    // std::call_once(onceFlag, get_all_items_similarity, k);

    ItemSet& interestedItems = user->interestedItemSet();
    if (interestedItems.empty()) {
        LOG(INFO) << "Target user " << user->ID() << " do not have histroy interests record, cannot recommend!";
        return 0;
    } // if

    std::map<Item*, float, ItemPtrCmp> rankMap;
    for (Item *pItemI : interestedItems) {
        auto& similarItems = pItemI->similarItems();
        for (auto &sItemJ : similarItems) {
            if (interestedItems.find(sItemJ.pOther) != interestedItems.end())
                continue;
            rankMap[sItemJ.pOther] += sItemJ.similarity; 
        } // for j
    } // for i

    rcmdItems.reserve( rankMap.size() );
    for (auto &v : rankMap)
        rcmdItems.push_back( RcmdItem(v.first, v.second) );

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


void get_all_items_similarity( std::size_t k )
{
    using namespace std;

    auto err_ret = [](int retval, const char *msg) {
        cerr << msg << endl;
        return retval;
    };

    if (!k)
        err_ret(0, "Invalid k value!");
    
    LOG(INFO) << "get_all_items_similarity start...";

    size_t nAllItems = g_pItemDB->size();
    vector<Item*> allItems;
    allItems.reserve(nAllItems);

    const auto &itemDbContent = g_pItemDB->content();
    for (uint32_t i = 0; i != ItemDB::HASH_SIZE; ++i) {
        const auto &elem = itemDbContent[i];
        for (const auto &v : elem)
            allItems.push_back( v.second.get() );
    } // for i

    // cout << allItems.size() << endl;
    // cout << g_pItemDB->size() << endl;
    // cout << "get_all_items_similarity done!" << endl;

    auto get_item_similarity = []( Item *pItemI, Item *pItemJ )->float {
        UserSet& Ni = pItemI->interestedUserSet();
        UserSet& Nj = pItemJ->interestedUserSet();
        vector<User*> Nij;
        Nij.reserve(Ni.size());
        set_intersection( Ni.begin(), Ni.end(), Nj.begin(), Nj.end(),
                          back_inserter(Nij) );

        if (Nij.empty())
            return 0.0;

        float similarity = 0.0;
        for (User *u : Nij) {
            size_t sz = u->interestedItemSet().size();
            if (!sz) 
                continue;
            similarity += get_factor(sz);
        } // for

        similarity /= std::sqrt( (float)(Ni.size() * Nj.size()) );

        return similarity;
    };

    // struct SimilarityJob {
        // SimilarityJob( size_t _i, size_t _j )
                // : i(_i), j(_j) {}
        // size_t i, j;
    // };
    
    auto similarityJob = [&](size_t i, size_t j) {
        float similarity = get_item_similarity( allItems[i], allItems[j] );
        allItems[i]->addSimilarItem( allItems[j], similarity, k );
        allItems[j]->addSimilarItem( allItems[i], similarity, k );
    };

    typedef std::function<void(void)>   JobType;
    ThreadPool<JobType> thrpool( g_nMaxThread );

    for (size_t i = 0; i < nAllItems-1; ++i) {
        for (size_t j = i+1; j < nAllItems; ++j) {
            thrpool.addJob( std::bind(similarityJob, i, j) );
            // auto similarityJob = ;
        } // for j
    } // for i

    thrpool.terminate();

/*
 * #pragma omp parallel for
 *     for (size_t i = 0; i < nAllItems-1; ++i) {
 * #pragma omp parallel for
 *         for (size_t j = i+1; j < nAllItems; ++j) {
 *             // LOG(INFO) << "computing similarity between " << i << " and " << j;
 *             float similarity = get_item_similarity( allItems[i], allItems[j] );
 *             allItems[i]->addSimilarItem( allItems[j], similarity, k );
 *             allItems[j]->addSimilarItem( allItems[i], similarity, k );
 *         } // for j
 *     } // for i
 */

    LOG(INFO) << "get_all_items_similarity done!";

    return;
}


/*
 * void get_all_items_similarity()
 * {
 *     auto&        userDbTable = g_pUserDB->content();
 *     uint32_t     idx = 0;
 *     boost::mutex idxMtx;
 * 
 *     auto processUser = [&](const User_sptr &pUser) {
 *         ItemSet itemSet;
 *         pUser->interestedItems( itemSet );
 *         std::vector<Item_sptr> items( itemSet.begin(), itemSet.end() );
 *         float value = 1.0 / std::log(1.0 + items.size());
 * 
 *         for (std::size_t i = 0; i != items.size()-1; ++i) {
 *             for (std::size_t j = i + 1; j != items.size(); ++j) {
 *                 auto &si = items[i]->similarItems();
 *                 auto &sj = items[j]->similarItems();
 *                 boost::unique_lock< std::remove_reference<decltype(si)>::type > lockI(si);
 *                 boost::unique_lock< std::remove_reference<decltype(sj)>::type > lockJ(sj);
 *                 si[items[j]->ID()] += value;
 *                 sj[items[i]->ID()] += value;
 *             } // for j
 *         } // for i
 *     };
 * 
 *     auto threadRoutine = [&] {
 *         while (true) {
 *             boost::unique_lock< boost::mutex >  lock(idxMtx);
 *             if (idx == UserDB::HASH_SIZE)
 *                 return;
 *             auto& uMap = userDbTable[idx++];
 *             lock.unlock();
 * 
 *             for (auto &v : uMap) {
 *                 User_sptr pUser = v.second;
 *                 processUser( pUser );
 *             } // for
 *         } // while
 *     };
 * 
 *     boost::thread_group thrgroup;
 *     for( uint32_t i = 0; i < g_nMaxThread; ++i )
 *         thrgroup.create_thread( threadRoutine );
 *     thrgroup.join_all();
 * }
 */

