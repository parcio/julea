#include <sstream>
#include <string>

using namespace std;

namespace JULEA
{
	namespace Common
	{
		string BuildFilename (string const&, string const&);
		template <typename T>
		string ToString (T const& v)
		{
			stringstream s;
			s << v;
			return s.str();
		}
	}
}
