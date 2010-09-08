#include <exception>
#include <list>

using namespace std;

namespace JULEA
{
	class Exception : public exception
	{
		public:
			Exception (string const& description) throw() : description(description) {};
			~Exception () throw() {};
			const char* what () const throw() { return description.c_str(); };
		private:
			string description;
	};

	class FileSystem
	{
		public:
			static void Initialize (string const &host) { FileSystem::host = host; };
			static string const& Host ();
		private:
			static string host;
	};

	class File
	{
		public:
			File () {};
			File (string const& name) : name(name) {};
			~File () {};
			string const& Name ();
		private:
			string name;
	};

	class Directory
	{
		public:
			Directory (string const& path) : path(path) { eid.clear(); id.clear(); };
			~Directory () {};
			bool Create ();
			bool CreateFiles();
			void Add (Directory const&);
			void Add (File const&);
		private:
			mongo::OID eid;
			mongo::OID id;
			string path;
			std::list<Directory> newDirectories;
			std::list<File> newFiles;
	};
}
