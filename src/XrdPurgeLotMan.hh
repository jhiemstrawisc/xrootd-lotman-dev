#ifndef __XRDPURGELOTMAN_HH__
#define __XRDPURGELOTMAN_HH__

#include <XrdPfc/XrdPfcPurgePin.hh>
#include <XrdPfc/XrdPfcDirStateSnapshot.hh>
#include <XrdPfc/XrdPfc.hh>
#include <unordered_set>

namespace XrdPfc
{

enum class PurgePolicy {
    PastDel,
    PastExp,
    PastOpp,
    PastDed
};

std::string getPolicyName(PurgePolicy policy) {
    switch (policy) {
        case PurgePolicy::PastDel:
            return "LotsPastDel";
        case PurgePolicy::PastExp:
            return "LotsPastExp";
        case PurgePolicy::PastOpp:
            return "LotsPastOpp";
        case PurgePolicy::PastDed:
            return "LotsPastDed";
        default:
            return "UnknownPolicy";
    }
}

class XrdPurgeLotMan : public PurgePin
{
public:
    XrdPurgeLotMan();
    virtual ~XrdPurgeLotMan() override;

    const Configuration &conf = Cache::Conf();

    virtual long long GetBytesToRecover(const DataFsPurgeshot&) override;
    virtual bool ConfigPurgePin(const char* params) override;

    // Policy implementations
    void lotsPastDelPolicy(const DataFsPurgeshot&, long long &bytesToRecover);
    void lotsPastExpPolicy(const DataFsPurgeshot&, long long &bytesRemaining);
    void lotsPastOppPolicy(const DataFsPurgeshot &purgeShot, long long &bytesRemaining);
    void lotsPastDedPolicy(const DataFsPurgeshot &purgeShot, long long &bytesRemaining);

    static long long getTotalUsageB();
    std::map<std::string, long long> lotPerDirUsageB(const std::string &lot, const DataFsPurgeshot &purge_shot);

    long long GetConfiguredHWM();
    long long GetConfiguredLWM();

    

protected:
    std::unordered_set<std::string> m_dirs_to_purge;
    std::map<std::string, long long> m_dirs_to_purge_b_remaining;

private:
    // indicates that these tend to clean out an entire lot, such as lots past deletion/expiration
    void completePurgePolicyBase(const DataFsPurgeshot &purgeShot, long long &bytesRemaining, PurgePolicy policy);
    // whereas these only purge some of the storage, such as lots past opportunistic/dedicated storage
    void partialPurgePolicyBase(const DataFsPurgeshot &purgeShot, long long &bytesRemaining, PurgePolicy policy);

};

}

#endif // __XRDPURGELOTMAN_HH__