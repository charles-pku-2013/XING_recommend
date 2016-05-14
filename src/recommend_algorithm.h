#ifndef _RECOMMEND_ALGORITHM_H_
#define _RECOMMEND_ALGORITHM_H_

#include "common.h"

/**
 * @brief 
 *
 * @param user          目标用户
 * @param k             与user最相似的k个用户
 * @param nItems        最多推荐物品数
 * @param recommended   推荐结果
 * @return              实际推荐的item个数，<= nItems
 */
extern std::size_t UserCF( User *user, std::size_t k, std::size_t nItems,
                           std::vector<RcmdItem> &rcmdItems );


extern void get_all_items_similarity(std::size_t);
extern std::size_t ItemCF( User *user, std::size_t k, std::size_t nItems,
                           std::vector<RcmdItem> &rcmdItems );

#endif

