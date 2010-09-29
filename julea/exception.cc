#include <iostream>

#include <boost/filesystem.hpp>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "exception.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	Exception::Exception (string const& description) throw()
		: m_description(description)
	{
	}

	Exception::~Exception () throw()
	{
	}

	const char* Exception::what () const throw()
	{
		return m_description.c_str();
	}
}
