#include <unistd.h>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "credentials.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	Credentials::Credentials ()
	{
		m_user = geteuid();
		m_group = getegid();
	}

	Credentials::Credentials (int user, int group)
		: m_user(user), m_group(group)
	{
	}

	Credentials::~Credentials ()
	{
	}

	int Credentials::User ()
	{
		return m_user;
	}

	int Credentials::Group ()
	{
		return m_group;
	}
}
