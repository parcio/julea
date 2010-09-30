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
			Credentials (uint32_t, uint32_t);
			~Credentials ();
		private:
			uint32_t m_user;
			uint32_t m_group;
	};
}

#endif
