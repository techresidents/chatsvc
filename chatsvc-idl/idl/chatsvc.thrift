namespace java com.techresidents.services.chatsvc.gen
namespace py techresidents.services.chatsvc.gen

include "core.thrift"

service TChatService
{
    string getVersion(1:core.RequestContext requestContext),
    string getBuildNumber(1:core.RequestContext requestContext)
}
