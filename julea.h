#include <list>

namespace JULEA
{
	class FileSystem
	{
		public:
			static void Initialize (string const &host) { FileSystem::host = host; };
			static string const& Host () { return FileSystem::host; }
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
