#include "XrdPurgeLotMan.hh" // Include the header file for XrdPurgeLotMan
#include <lotman/lotman.h>
#include <nlohmann/json.hpp>

#include "XrdPfc/XrdPfcPurgePin.hh"
#include <XrdPfc/XrdPfcDirStateSnapshot.hh>

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucUtils.hh"
#include "XrdOuc/XrdOucStream.hh"

#include <iostream>
#include <string>
#include <sstream>

#include <filesystem>

#define GB2B 1024ll * 1024ll * 1024ll
#define BLKSZ 512ll

namespace fs = std::filesystem;
using json = nlohmann::json;
namespace {
std::string stripQuotes(std::string str) {
    // Check if the first character is a quotation mark
    if (!str.empty() && str.front() == '"') {
        str.erase(0, 1); // Remove the first character
    }
    // Check if the last character is a quotation mark
    if (!str.empty() && str.back() == '"') {
        str.erase(str.size() - 1); // Remove the last character
    }
    return str;
}


void printStringArray(char **arr) {
    if (arr == nullptr) {
        std::cout << "The array is null." << std::endl;
        return;
    }
    for (int i = 0; arr[i] != nullptr; ++i) {
        std::cout << arr[i] << std::endl;
    }
}

// Custom deleter for unique pointers in which LM allocates some memory
// Used to guarantee we call `lotman_free_string_list` on these pointers
struct LotDeleter {
    void operator()(char** ptr) {
        lotman_free_string_list(ptr);
    }
};


std::map<std::string, long long> getLotUsageMap(char ***lots)
{
    std::map<std::string, long long> lots_to_recover;
    for (int i = 0; *lots[i] != nullptr; ++i)
    {

        json usageQueryJSON;
        std::string lot_name = *lots[i];

        usageQueryJSON["lot_name"] = lot_name;
        usageQueryJSON["total_GB"] = true;

        char *output;
        char *err;
        auto rv = lotman_get_lot_usage(usageQueryJSON.dump().c_str(), &output, &err);
        if (rv != 0)
        {
            std::cout << "Error getting lot usage for " << lot_name << ": " << err << std::endl;
            continue;
        }

        std::cout << "   Usage for " << lot_name << ": " << output << std::endl;

        long long bytes_to_recover = 0;
        // Parse the output to get the bytes to recover
        json usageJSON = json::parse(output);
        std::cout << "usageJSON: " << usageJSON.dump(4) << std::endl;
        if (usageJSON.find("total_GB") != usageJSON.end())
        {
            double totalGB = usageJSON["total_GB"]["total"];
            bytes_to_recover = static_cast<long long>(totalGB * GB2B);
        }
        std::cout << "BYTES TO RECOVER FROM LOT " << lot_name << ": " << bytes_to_recover << std::endl;
        lots_to_recover[lot_name] = bytes_to_recover;
    }

    return lots_to_recover;
}

struct DirNode {
    std::filesystem::path path;
    std::vector<DirNode*> subDirs; // Pointers to subdirectories
};

json dirNodeToJson(const DirNode* node, const XrdPfc::DataFsPurgeshot &purge_shot) {
    nlohmann::json dirJson;
    std::filesystem::path dirPath(node->path);
    dirJson["path"] = dirPath.filename().string();
    std::cout << "   USAGE FOR " << dirPath.string() << " IS " << static_cast<double>(purge_shot.find_dir_usage_for_dir_path(dirPath.string())->m_StBlocks) * BLKSZ  / (GB2B) << std::endl;
    dirJson["size_GB"] = static_cast<double>(purge_shot.find_dir_usage_for_dir_path(dirPath.string())->m_StBlocks) * BLKSZ  / (GB2B);
    
    if (!node->subDirs.empty()) {
        dirJson["includes_subdirs"] = true;
        for (const auto* subDir : node->subDirs) {
            dirJson["subdirs"].push_back(dirNodeToJson(subDir, purge_shot));
        }
    } else {
        dirJson["includes_subdirs"] = false;
    }

    return dirJson;
}

json reconstructPathsAndBuildJson(const XrdPfc::DataFsPurgeshot &purge_shot) {
    std::unordered_map<int, DirNode> indexToDirNode;
    std::vector<DirNode*> rootDirs;

    for (int i = 0; i < purge_shot.m_dir_vec.size(); ++i) {
        const auto& dir_entry = purge_shot.m_dir_vec[i];
        DirNode& dirNode = indexToDirNode[i];
        dirNode.path = dir_entry.m_dir_name;

        if (dir_entry.m_parent != -1) {
            dirNode.path = std::filesystem::path("/") / indexToDirNode[dir_entry.m_parent].path / dirNode.path;
            indexToDirNode[dir_entry.m_parent].subDirs.push_back(&dirNode);
            if (dir_entry.m_parent == 0) {

                rootDirs.push_back(&dirNode);
            }
        } 
    }

    nlohmann::json allDirsJson = nlohmann::json::array();
    for (const auto* rootDir : rootDirs) {
        allDirsJson.push_back(dirNodeToJson(rootDir, purge_shot));
    }

    return allDirsJson;
}



} // End of anonymous namespace

namespace XrdPfc { 

XrdPurgeLotMan::XrdPurgeLotMan() :
    m_purge_dirs{} {}

XrdPurgeLotMan::~XrdPurgeLotMan()
{
}

long long XrdPurgeLotMan::GetConfiguredHWM()
{
    return conf.m_diskUsageHWM;
}

long long XrdPurgeLotMan::GetConfiguredLWM()
{
    return conf.m_diskUsageLWM;
}

long long XrdPurgeLotMan::getTotalUsageB()
{
    // Get all lots, and aggregate those that are rootly
    char **rawLots = nullptr;
    char *err;
    auto rv = lotman_list_all_lots(&rawLots, &err);
    // After the call, wrap the raw pointer with std::unique_ptr
    std::unique_ptr<char*[], LotDeleter> lots(rawLots, LotDeleter());
    if (rv != 0)
    {
        std::cout << "Error getting all lots: " << err << std::endl;
        // CAREFUL WITH 0 RETURN. We'll never recover any space.
        return 0;
    }

    // For each lot, check if it's a root lot, and if so get its total usage
    long long totalUsage = 0;
    for (int i = 0; lots[i] != nullptr; ++i)
    {
        std::string lotName = lots[i];
        // Check if the lot is a root lot
        if (int rc = lotman_is_root(lotName.c_str(), &err); rc != 1) {
            // Not root, or an error
            if (rc < 0) {
                std::cout << "Error checking if lot is root: " << err << std::endl;
            }

            continue;
        } else {
            // Root lot. Get the total usage.
            json usageQueryJSON;
            usageQueryJSON["lot_name"] = lotName;
            usageQueryJSON["total_GB"] = true;

            char *output;
            rv = lotman_get_lot_usage(usageQueryJSON.dump().c_str(), &output, &err);
            if (rv != 0)
            {
                continue;
            }

            json usageJSON = json::parse(output);
            double totalGB = usageJSON["total_GB"]["total"];
            totalUsage += static_cast<long long>(totalGB * GB2B);

        }
    }

    return totalUsage;
}

// std::vector<std::pair<std::string, long long>> XrdPurgeLotMan::lotPerDirUsageB(const std::string &lot, const DataFsPurgeshot &purge_shot) {
std::map<std::string, long long> XrdPurgeLotMan::lotPerDirUsageB(const std::string &lot, const DataFsPurgeshot &purge_shot) {

    // std::vector<std::pair<std::string, long long>> usageMap;
    std::map<std::string, long long> usageMap;

    // Get all the directories in the lot
    char *dirs; // will hold a JSON list of objects
    char *err;

    auto rv = lotman_get_lot_dirs(lot.c_str(), true, &dirs, &err);
    if (rv != 0)
    {
        std::cout << "Error getting dirs in lot " << lot << ": " << err << std::endl;
        return usageMap;
    }


    json dirsJSON = json::parse(dirs);
    // Iterate through JSON list of objects, get the name of each directory
    for (const auto& dir : dirsJSON)
    {
        std::string path = dir["path"];
        // Get the usage for the directory
        const DirUsage* dirUsage = purge_shot.find_dir_usage_for_dir_path(path);
        if (dirUsage == nullptr)
        {
            std::cout << "Error finding usage for directory " << path << std::endl;
            continue;
        }
        long long bytesToRecover = static_cast<long long>(dirUsage->m_StBlocks) * BLKSZ;
        usageMap[path] = bytesToRecover;
        // usageMap.push_back(std::make_pair(path, bytesToRecover));
    }

    return usageMap;
}


void XrdPurgeLotMan::lotsPastDelPolicy(const DataFsPurgeshot &purgeShot, long long &bytesRemaining)
{
    PurgePolicy policy = PurgePolicy::PastDel;
    completePurgePolicyBase(purgeShot, bytesRemaining, policy);
}

void XrdPurgeLotMan::lotsPastExpPolicy(const DataFsPurgeshot &purgeShot, long long &bytesRemaining)
{
    PurgePolicy policy = PurgePolicy::PastExp;
    completePurgePolicyBase(purgeShot, bytesRemaining, policy);
}

void XrdPurgeLotMan::lotsPastOppPolicy(const DataFsPurgeshot &purgeShot, long long &bytesRemaining)
{
    PurgePolicy policy = PurgePolicy::PastOpp;
    partialPurgePolicyBase(purgeShot, bytesRemaining, policy);
}

void XrdPurgeLotMan::lotsPastDedPolicy(const DataFsPurgeshot &purgeShot, long long &bytesRemaining)
{
    PurgePolicy policy = PurgePolicy::PastDed;
    partialPurgePolicyBase(purgeShot, bytesRemaining, policy);
}


void XrdPurgeLotMan::completePurgePolicyBase(const DataFsPurgeshot &purgeShot, long long &globalBRemaining, XrdPfc::PurgePolicy policy) 
{
    char **lots;
    char *err;
    int rv;
    switch(policy)
    {
        case XrdPfc::PurgePolicy::PastDel:
            rv = lotman_get_lots_past_del(true, &lots, &err);
            break;
        case XrdPfc::PurgePolicy::PastExp:
            rv = lotman_get_lots_past_exp(true, &lots, &err);
            break;
    }

    if (rv != 0)
    {
        std::cout << "Error getting lots for policy " << getPolicyName(policy) << ": " << err << std::endl;
        lotman_free_string_list(lots);
        return;
    }
    std::cout << "LOTS FOR POLICY " << getPolicyName(policy) << ": " << std::endl;
    printStringArray(lots);

    // Get directory usage for each of the directories tied to each lot
    for (int i = 0; lots[i] != nullptr; ++i)
    {
        if (globalBRemaining == 0) {
            break;
        }

        const std::string lotName = lots[i];

        std::map<std::string, long long> tmp_map = lotPerDirUsageB(lotName, purgeShot);
        for (const auto& [dir, bytesInDir] : tmp_map)
        {
            if (globalBRemaining <= 0) {
                break;
            }

            long long toRecoverFromDir;
            if (m_purge_dirs.find(dir) != m_purge_dirs.end()) {
                // There's nothing left to clean up in this directory
                if (m_purge_dirs[dir]->dir_b_remaining <= 0) {
                    continue;
                }
                // Clean out the rest of the dir, unless we don't have that much left to clear
                toRecoverFromDir = std::min(m_purge_dirs[dir]->dir_b_remaining, globalBRemaining);
            } else {
                toRecoverFromDir = std::min(bytesInDir, globalBRemaining);
                m_purge_dirs[dir] = std::make_unique<PurgeDirCandidateStats>(PurgeDirCandidateStats(0ll, bytesInDir));
            }


            m_purge_dirs[dir]->dir_b_to_purge += toRecoverFromDir;
            globalBRemaining -= toRecoverFromDir;
            m_purge_dirs[dir]->dir_b_remaining -= toRecoverFromDir;

            std::cout << "   ADDING dir: " << dir << std::endl <<
                "   dirUsage: " << bytesInDir <<  std::endl << 
                "   bytes_to_purge: " << m_purge_dirs[dir]->dir_b_to_purge << std::endl << 
                "   bytes_remaining: " << m_purge_dirs[dir]->dir_b_remaining << std::endl <<
                "   toRecoverFromDir: " << toRecoverFromDir << std::endl <<
                "   GlobalBRemaining: " << globalBRemaining << std::endl;
        }
    }

    std::cout << "   TOTAL B REMAINING AFTER " << getPolicyName(policy) << " POLICY: " << globalBRemaining << std::endl;


    return;

}


void XrdPurgeLotMan::partialPurgePolicyBase(const DataFsPurgeshot &purgeShot, long long &globalBRemaining, XrdPfc::PurgePolicy policy) 
{
    char **lots;
    char *err;
    // TODO: Come back and think about whether we want recursive children here
    //       For now, I'm saying _yes_ because if a child takes up lots of space
    //       but isn't past its own quota, we still want the option to clear it.
    int rv;
    switch(policy)
    {
        case XrdPfc::PurgePolicy::PastOpp:
            rv = lotman_get_lots_past_opp(true, true, &lots, &err);
            break;
        case XrdPfc::PurgePolicy::PastDed:
            rv = lotman_get_lots_past_ded(true, true, &lots, &err);
            break;
    }
    if (rv != 0)
    {
        std::cout << "Error getting lots for policy " << getPolicyName(policy) << ": " << err << std::endl;
        lotman_free_string_list(lots);
        return;
    }
    std::cout << "LOTS FOR POLICY " << getPolicyName(policy) << ": " << std::endl;
    printStringArray(lots);

    // Get directory usage for each of the directories tied to each lot
    for (int i = 0; lots[i] != nullptr; ++i)
    {
        if (globalBRemaining <= 0) {
            break;
        }

        const std::string lotName = lots[i];

        // if past opp = total_usage - opp_usage - ded_usage
        // if past ded = total_usage - ded_usage
        long long toRecoverFromLot;
        // Get the total usage for the lot
        json usageQueryJSON;
        usageQueryJSON["lot_name"] = lotName;
        usageQueryJSON["total_GB"] = true;
        usageQueryJSON["dedicated_GB"] = true;
        if (policy == XrdPfc::PurgePolicy::PastOpp) {
            usageQueryJSON["opportunistic_GB"] = true;
        }

        char *output;
        rv = lotman_get_lot_usage(usageQueryJSON.dump().c_str(), &output, &err);
        if (rv != 0)
        {
            std::cout << "Error getting lot usage for " << lotName << ": " << err << std::endl;
            continue;
        }

        json usageJSON = json::parse(output);
        double totalGB = usageJSON["total_GB"]["total"];
        double dedGB = usageJSON["dedicated_GB"]["total"];
        if (policy == XrdPfc::PurgePolicy::PastOpp) {
            double oppGB = usageJSON["opportunistic_GB"]["total"];
            toRecoverFromLot = static_cast<long long>((totalGB - dedGB - oppGB) * GB2B);
            std::cout << "   TO RECOVER FROM LOT: " << lotName << " IS " << toRecoverFromLot << std::endl;
            std::cout << "   TOTAL GB: " << totalGB << " DED GB: " << dedGB << " OPP GB: " << oppGB << std::endl;


        } else {
            toRecoverFromLot = static_cast<long long>((totalGB - dedGB) * GB2B);
            std::cout << "   TO RECOVER FROM LOT: " << lotName << " IS " << toRecoverFromLot << std::endl;
            std::cout << "   TOTAL GB: " << totalGB << " DED GB: " << dedGB  << std::endl;
        }

        if (toRecoverFromLot > globalBRemaining) {
            toRecoverFromLot = globalBRemaining;
        }

        std::map<std::string, long long> tmp_usage = lotPerDirUsageB(lotName, purgeShot);
        for (const auto& [dir, bytesInDir] : tmp_usage)
        {
            if (globalBRemaining <= 0 || toRecoverFromLot <= 0) {
                break;
            }

            long long toRecoverFromDir;
            if (m_purge_dirs.find(dir) != m_purge_dirs.end()) {
                // There's nothing left to clean up in this directory
                if (m_purge_dirs[dir]->dir_b_remaining <= 0) {
                    continue;
                }

                // there's space left to clear. Get rid of as much of it as we need to
                toRecoverFromDir = std::min(m_purge_dirs[dir]->dir_b_remaining, toRecoverFromLot);
            } else {
                // First time we've seen this dir as a candidate, record it
                toRecoverFromDir = std::min(bytesInDir, toRecoverFromLot);
                m_purge_dirs[dir] = std::make_unique<PurgeDirCandidateStats>(PurgeDirCandidateStats(0ll, bytesInDir));
            }


            m_purge_dirs[dir]->dir_b_to_purge += toRecoverFromDir;
            globalBRemaining -= toRecoverFromDir;
            toRecoverFromLot -= toRecoverFromDir;
            m_purge_dirs[dir]->dir_b_remaining -= toRecoverFromDir;
            std::cout << "   ADDING dir: " << dir << std::endl <<
                "   dirUsage: " << bytesInDir <<  std::endl << 
                "   bytes_to_purge: " << m_purge_dirs[dir]->dir_b_to_purge << std::endl << 
                "   bytes_remaining: " << m_purge_dirs[dir]->dir_b_remaining << std::endl <<
                "   toRecoverFromDir: " << toRecoverFromDir << std::endl <<
                "   toRecoverFromLot: " << toRecoverFromLot << std::endl << 
                "   GlobalBRemaining: " << globalBRemaining << std::endl;


        }
    }

    // std::cout << "   Added dirs to purge list for " << getPolicyName(policy) << " policy." << std::endl;
    // for (const auto & [dir, stats] : m_purge_dirs) {
    //     std::cout << "      dir: " << dir << " bytes_to_purge: " << stats->bytes_to_purge << " bytes_remaining: " << stats->bytes_remaining << std::endl;
    // }

    std::cout << "   TOTAL B REMAINING AFTER " << getPolicyName(policy) << " POLICY: " << globalBRemaining << std::endl;
    return;

}





/*
Handles determining the total number of bytes to recover,
as well as populating the m_list of directories:bytesToRecover the purge cycle
iterates over when clearing stuff.

In LotMan's ideal case, we could say "here's the ordering of directories, keep clearing
until you hit LWM," but the purge code doesn't quite have the logic for that, so handle this
determination in the plugin.
*/
long long XrdPurgeLotMan::GetBytesToRecover(const DataFsPurgeshot &purge_shot)
{
    // reset m_list
    m_list.clear();
    
    // Get all the lots
    char **lots;
    char *err;
    char *output;
    auto rv = lotman_get_context_str("lot_home", &output, &err);
    if (rv != 0)
    {
        std::cerr << "Error getting lot home: " << err << std::endl;
        return 0;
    }
    auto lotUpdateJson = reconstructPathsAndBuildJson(purge_shot);

    rv = lotman_update_lot_usage_by_dir(lotUpdateJson.dump().c_str(), false, &err);
    if (rv != 0)
    {
        std::cout << "Error updating lot usage by dir: " << err << std::endl;
        return 0;
    }


    // Get the total usage across root lots.
    long long totalUsageB = getTotalUsageB();

    std::cout << "   TOTAL USAGE B: " << totalUsageB << std::endl;

    if (totalUsageB < GetConfiguredHWM())
    {
        std::cout << std::endl << std::endl << "Total usage is below the LWM." << std::endl << std::endl;
        return 0;
    }


    // We've determined there's something to purge
    long long bytesToRecover = totalUsageB - conf.m_diskUsageLWM;
    std::cout << "   BYTES TO RECOVER: " << bytesToRecover << std::endl << std::endl;

    long long bytesRemaining = bytesToRecover;



    applyPolicies(purge_shot, bytesRemaining);

    std::cout << std::endl << "   BYTES TO RECOVER: " << bytesToRecover << std::endl;

    // TODO: Come back and consolidate this with the other member map to avoid duplicating keys like this.
    for (const auto & [dir, stats] : m_purge_dirs) {
        DirInfo update;
        update.path = dir + "/"; // XRootD will seg fault if the path doesn't end in a slash
        update.nBytesToRecover = stats->dir_b_to_purge;

        m_list.push_back(update);
    }

    for (const auto &dir : m_list)
    {
        std::cout << "   DIR: " << dir.path << " nBytesToRecover: " << dir.nBytesToRecover << std::endl;
    }

    return bytesToRecover;
}

bool XrdPurgeLotMan::validateConfiguration(const char *params)
{
    LotManConfiguration cfg{};

    // Split params by space
    std::istringstream iss(params);
    std::vector<std::string> paramVec;
    std::string token;
    char delim = ' ';
    
    while (getline(iss, token, delim)) { 
        paramVec.push_back(token); 
    } 

    for (const auto& part : paramVec) { 
        std::cout << part << std::endl; 
    } 

    assert(paramVec.size() >= 1);

    // Get LotHome
    std::filesystem::path lot_home(paramVec[0]);
    if (!std::filesystem::exists(lot_home) && !std::filesystem::is_directory(lot_home))
    {
        std::cout << "Lot home does not exist." << std::endl;
        return false;
    }
    cfg.SetLotHome(lot_home.string());

    std::vector<PurgePolicy> policies;
    for (int i = 1; i < paramVec.size(); ++i)
    {
        PurgePolicy policy = getPolicyFromName(paramVec[i]);
        if (policy == PurgePolicy::UnknownPolicy)
        {
            std::cout << "Unknown policy: " << paramVec[i] << std::endl;
            return false;
        }
        policies.push_back(policy);
    }


    // Use default policies if none are provided
    if (policies.empty())
    {
        policies = {PurgePolicy::PastDel, PurgePolicy::PastExp, PurgePolicy::PastOpp, PurgePolicy::PastDed};
    }

    cfg.SetPolicy(policies);

    m_lotman_conf = cfg;

    return true;
}

bool XrdPurgeLotMan::ConfigPurgePin(const char* params)
{
    (void)params; // Avoid unused parameter warning

    std::cout << std::endl << std::endl << "PARAMS: " << params << std::endl << std::endl;

    if (!validateConfiguration(params)) {
        std::cout << "COULD NOT VALIDATE CONFIGURATION" << std::endl;
        return false;
    };
    
    char *err;
    auto rv = lotman_set_context_str("lot_home", getLotHome().c_str(), &err);
    if (rv != 0)
    {
        std::cout << "Error setting lot home: " << err << std::endl;
        return false;
    }



    return true;
}

} // End of namespace XrdPfc


/******************************************************************************/
/*                          XrdPfcGetPurgePin                                 */
/******************************************************************************/

// Return a purge object to use.
extern "C"
{
   XrdPfc::PurgePin *XrdPfcGetPurgePin(XrdSysError &)
   {
      return new XrdPfc::XrdPurgeLotMan();
   }
}