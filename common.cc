#include "common.h"

using namespace std;

namespace JULEA
{
	namespace Common
	{
		string BuildFilename (string const& p1, string const& p2)
		{
			if (p1 == "/")
			{
				return string(p1 + p2);
			}

			return string(p1 + "/" + p2);
		}
	}
}
