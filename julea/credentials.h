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
			unsigned int user;
			unsigned int group;
	};
}

#endif
