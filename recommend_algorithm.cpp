#include "recommend_algorithm.h"


void UserCF( const User_sptr &user, int k, int nItems, 
                    std::vector<Item_sptr> &recommended )
{
    using namespace std;

    // first, find all items that "user" has positive interactions.
    ItemSet     userInterested;
    user->interestedItems( userInterested );

    // test
    /*
     * for (const auto &i : userInterested)
     *     cout << i->ID() << " ";
     * cout << endl;
     * cout << "size of userInterested is: " << userInterested.size() << endl;
     */

    // 对N(u)中的每一个物品 i∈N(u), 找出i的兴趣用户集合N(i)
    for (const Item_sptr &pItem : userInterested) {
        ;
    } // for
}
