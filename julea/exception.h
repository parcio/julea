#ifndef H_EXCEPTION
#define H_EXCEPTION

#include <exception>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

namespace JULEA
{
	class Exception;
}

namespace JULEA
{
	class Exception : public std::exception
	{
		public:
			Exception (string const&) throw();
			~Exception () throw();

			const char* what () const throw();
		private:
			string m_description;
	};
}

#endif
