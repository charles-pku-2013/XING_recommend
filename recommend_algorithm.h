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
 */
extern std::size_t UserCF( const User_sptr &user, std::size_t k, std::size_t nItems,
                           std::vector<RcmdItem> &rcmdItems );


#endif

