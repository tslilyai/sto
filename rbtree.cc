#define INTERVAL_TREE_DEBUG 1
#define rbaccount(x) ++rbaccount_##x
unsigned long long rbaccount_rotation, rbaccount_flip, rbaccount_insert, rbaccount_erase;
#include <iostream>
#include <utility>
#include <string.h>
#include "RBTree.hh"
#include <sys/time.h>
#include <sys/resource.h>

void rbaccount_report() {
    unsigned long long all = rbaccount_insert + rbaccount_erase;
    fprintf(stderr, "{\"insert\":%llu,\"erase\":%llu,\"rotation_per_operation\":%g,\"flip_per_operation\":%g}\n",
            rbaccount_insert, rbaccount_erase, (double) rbaccount_rotation / all, (double) rbaccount_flip / all);
}

int main() {
    {
        RBTree<int, int> tree;
        auto x = std::pair<int, int>(1,1);
        tree.insert(x);
    }
}