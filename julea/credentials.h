#ifndef H_CREDENTIALS
#define H_CREDENTIALS

namespace JULEA
{
	class Credentials;
}

namespace JULEA
{
	class Credentials
	{
		public:
			Credentials ();
			~Credentials ();
		private:
			uint32_t user;
			uint32_t group;
	};
}

#endif
