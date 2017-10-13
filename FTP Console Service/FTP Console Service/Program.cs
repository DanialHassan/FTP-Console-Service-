using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Net;
using System.Web;
using FtpLib;
using System.Text;
using System.IO.Compression;
using SevenZip;
using FtpLib;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Net.Mail;

namespace FTP_Console_Service
{
    class FTP_Console_Service
    {
        //public static string DMPath = @"D:\Hassan\", r = "Ratings";//, Dpbackup = "Backup", DPath = "Ratings";//172.168.100.198        
       // public static string email1, email2, smtp, from, to, subject, email, pwd, slen, elen;
        
        static string ip = System.Configuration.ConfigurationManager.AppSettings["IP"];//172.168.100.240       ftp.medialogic.com.pk
        public static string FTPServer = ip;

        static string name = System.Configuration.ConfigurationManager.AppSettings["User"];//ftpuser"               mm@medialogic.com.pk
        public static string userName = name;

        static string pname = System.Configuration.ConfigurationManager.AppSettings["Pwd"];//ftpmm9988              "blank6724"
        public static string password = pname;

        static string dm = System.Configuration.ConfigurationManager.AppSettings["DMp"];  //@D:\Hassan   
        public static string DMPath = dm;

        static string dr = System.Configuration.ConfigurationManager.AppSettings["DR"];// \Ratings  (D:\)
        public static string DPath =  dr;

        static string ftpr = System.Configuration.ConfigurationManager.AppSettings["ftpR"];// /Ratings (FTP)
        public static string folderName = ftpr;

        static string db = System.Configuration.ConfigurationManager.AppSettings["DB"];// \Backup (D:\)
        public static string Dpbackup = "/"+db;//error  DMPath + DPath + Dpbackup

        static string fpr = System.Configuration.ConfigurationManager.AppSettings["ftp"]; // FTP backup = /Backup
        public static string FTPbackup = fpr; // error

        static string sam = System.Configuration.ConfigurationManager.AppSettings["sm"]; //172.168.101.4
        public static string smtp = sam;

        static string fm = System.Configuration.ConfigurationManager.AppSettings["frm"]; //danial.ssuet@gmail.com
        public static string from = fm;

        static string ttt = System.Configuration.ConfigurationManager.AppSettings["too"];  //danialkhan90@yahoo.com
        public static string to = ttt;

        static string sb = System.Configuration.ConfigurationManager.AppSettings["sub"];  ///Test Mail"
        public static string subject = sb;

        static string em = System.Configuration.ConfigurationManager.AppSettings["emil"];  //danial.ssuet@gmail.com
        public static string email = em;

        static string pw = System.Configuration.ConfigurationManager.AppSettings["passwd"];  //*********
        public static string pwd = pw;
        public static string mailfilelist;

        public static string Totalfiles = "",recordcounter = "";
        public static DataTable dtcm;                
        static ConnectionStringSettings connect = ConfigurationManager.ConnectionStrings["dbcString"];
        static MySqlConnection dbcon = new MySqlConnection(connect.ConnectionString);
        static string constr = connect.ConnectionString;
        public static string f;
        
        public static void CursorTest()
        {
            int testsize = 1000000;
            Console.WriteLine("Testing cursor position");
            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < testsize; i++)
            {
                Console.Write("\rCounting: {0}     ", i);
            }
            sw.Stop();
            Console.WriteLine("\nTime using \\r: {0}", sw.ElapsedMilliseconds);
            sw.Reset();
            sw.Start();
            int top = Console.CursorTop;
            for (int i = 0; i < testsize; i++)
            {
                Console.SetCursorPosition(0, top);
                Console.Write("Counting: {0}     ", i);
            }
            sw.Stop();
            Console.WriteLine("\nTime using CursorLeft: {0}", sw.ElapsedMilliseconds);
            sw.Reset();
            sw.Start();
            Console.Write("Counting:          ");
            for (int i = 0; i < testsize; i++)
            {
                Console.Write("\b\b\b\b\b\b\b\b{0,8}", i);
            }
            sw.Stop();
            Console.WriteLine("\nTime using \\b: {0}", sw.ElapsedMilliseconds);
        }
        static void Main(string[] args)
        {
          ///  CursorTest();
            //return;
            string query = "SELECT mlchannelMapping.LogFileName, mlchannelMapping.RatingName, rechannel.channelID FROM mlchannelMapping Inner Join rechannel ON rechannel.channelName = mlchannelMapping.LogFileName";
            dtcm = GetData(query);//go to get data method

            string[] filelist;  
            filelist = GetFileList();// goto get file list method
            string a;
            int b = 0;
            try
            {
                if (filelist.Length > 1)
                {
                    foreach (string filename in filelist)
                    {
                        if (filename.Length >= 5)
                        {
                            if (filename.Substring(filename.Length - 4) == ".rar")
                            {
                                //a = filename.Substring(8, filename.Length - 8);
                                string dpwnloadPath = DMPath + @"\" +DPath;
                                Download(DMPath + DPath, filename);  //goto download method                    
                                b++;
                                Totalfiles = b.ToString();
                            }
                                else { }
                        }
                        else {} 
                    }

                    foreach (string filename in filelist)
                    {
                        if (filename.Length >= 5)
                        {
                            if (filename.Substring(filename.Length - 4) == ".rar")
                            {
                                mailfilelist = mailfilelist + filename + "\n";
                                a = filename.Substring(0, 18) + ".txt";
                                extract(DMPath + DPath, filename);  //goto extract method
                            }
                            else { }
                        }
                        else { }
                    }                      
                }
                    else { }
                ProcessFile(DMPath + DPath);
                foreach (string filename in filelist)
                {
                    if (filename.Length >= 5)
                    {
                        if (filename.Substring(filename.Length - 4) == ".rar")
                        {
                            a = filename.Substring(0, 18) + ".txt";
                            deletetxtfile(a);                        // delete txt files
                        }
                        else { }
                    }
                    else { }
                }

                foreach (string filename in filelist)
                {
                    if (filename.Length >= 5)
                    {
                        if (filename.Substring(filename.Length - 4) == ".rar")
                        {
                           // a = filename.Substring(7, filename.Length - 7);                           
                            ftpmove(filename);                        // ftp move
                        }
                        else { }
                    }
                    else { }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex + "No file found");
            }
            if (Totalfiles == "")
            { Console.WriteLine("No File Found in FTP Ratings"); }
            else
            {
                mail(mailfilelist);
                Console.WriteLine("\n\n\t\tTotal Files Process = " + Totalfiles + "\n\n\n\n");
            }
            Console.ReadKey();
        }
        public static DataTable GetData(string sqlquery)
        {
            if (dbcon.State != ConnectionState.Open) { dbcon.Open(); }
            MySqlDataAdapter da = new MySqlDataAdapter();
            da.SelectCommand = new MySqlCommand(sqlquery, dbcon);
            DataTable dt = new DataTable();
            da.Fill(dt);
            dbcon.Close();
            return dt;
        }

        public static string[] GetFileList()
        {
            string[] downloadFiles;
            StringBuilder result = new StringBuilder();
            FtpWebRequest reqFTP;
            try
            {
                reqFTP = (FtpWebRequest)FtpWebRequest.Create(new Uri("ftp://" + FTPServer + folderName));
                reqFTP.UseBinary = true;
                reqFTP.Credentials = new NetworkCredential(userName, password);
                reqFTP.Method = WebRequestMethods.Ftp.ListDirectory;
                WebResponse response = reqFTP.GetResponse();
                StreamReader reader = new StreamReader(response.GetResponseStream());                
                string line = reader.ReadLine();
                while (line != null)
                {
                    result.Append(line);
                    result.Append("\n");
                    line = reader.ReadLine();
                }
                if (result.ToString() == null)
                {
                    Console.WriteLine("No record found on FTP");
                }
                else
                {
                    result.Remove(result.ToString().LastIndexOf('\n'), 1);
                    reader.Close();
                    response.Close();
                    return result.ToString().Split('\n');
                }                  
                string[] arr1 = { };
                return arr1;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                downloadFiles = null;
                return downloadFiles;
            }
        }

        static void Download(string filePath, string fileName)
        {
            try
            {
                FtpWebRequest request = (FtpWebRequest)WebRequest.Create("ftp://ftp.medialogic.com.pk/Ratings/" + fileName); //ftp://ftp.medialogic.com.pk/Ratings/
                request.Method = WebRequestMethods.Ftp.DownloadFile;
                FileStream outputStream = new FileStream(filePath + "/" + fileName, FileMode.Create);
                // This example assumes the FTP site uses anonymous logon.
                request.Credentials = new NetworkCredential(userName, password);
                FtpWebResponse response = (FtpWebResponse)request.GetResponse();
                Stream responseStream = response.GetResponseStream();
                //StreamReader reader = new StreamReader(responseStream);
                long cl = response.ContentLength;
                int bufferSize = 1048;
                int readCount;
                int top = Console.CursorTop;
                byte[] buffer = new byte[bufferSize];
                readCount = responseStream.Read(buffer, 0, bufferSize);
                while (readCount > 0)
                {
                    outputStream.Write(buffer, 0, readCount);
                    readCount = responseStream.Read(buffer, 0, bufferSize);
                }
                //Console.WriteLine(reader.ReadToEnd());
                responseStream.Close();
                outputStream.Close();
                response.Close();
                Console.SetCursorPosition(0, top);
                Console.WriteLine("Download Complete{0} " , fileName);

                //reader.Close();
                response.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        static void extract(string Path, string filename)
        {
            try
            {    
                // for local DB
                 bool isExists = System.IO.Directory.Exists(Path + Dpbackup);
                bool present = System.IO.File.Exists(Path +Dpbackup + filename);
                if (!isExists)
                    System.IO.Directory.CreateDirectory(Path + "\\Backup");
                    
               
                    SevenZip.SevenZipExtractor.SetLibraryPath(@"C:\Program Files\7-Zip\7z.dll");
                    string fileName = filename.Substring(1, filename.Length - 1); // ext .txt file
                    string extpath = DMPath + DPath+"\\";
                    SevenZip.SevenZipExtractor exts = new SevenZip.SevenZipExtractor(extpath+filename);

                    exts.ExtractArchive(extpath);// file ext D:/Hassan/Ratings
                    //Console.WriteLine("File Extract");                   
                    f = filename.Substring(0, filename.Length - 5) + ".txt";
                    string sourcePath = DMPath + DPath; //source name
                    string targetPath = DMPath + DPath + Dpbackup; // targate name
                    // Use Path class to manipulate file and directory paths. 
                    string sourceFile = System.IO.Path.Combine(sourcePath, filename); //combile source and targate to move
                    string destFile = System.IO.Path.Combine(targetPath, filename);//combile destination and targate to move
                    string deletefile = System.IO.Path.Combine(sourcePath, f); // delete .txt file 
                    if (present)
                    {
                        System.IO.File.Move(sourceFile, destFile); // move file to backup folder 
                    }
                    else
                    {                       
                        System.IO.File.Delete(destFile);
                        System.IO.File.Move(sourceFile, destFile); // move file to backup folder
                    }
                
               // for Server
                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
         static void deletetxtfile(string fname)
        {
            string f = fname.Substring(0, fname.Length - 4) + ".txt";
            string sourcePath = DMPath + DPath; //source name
            string deletefile = System.IO.Path.Combine(sourcePath, f); // delete .txt file 
            System.IO.File.Delete(deletefile);// delete .txt files
        }
        public static string getChannelName(string rn)
        {
            string strExpr = "RatingName='" + rn + "'";
            DataRow[] dr = dtcm.Select(strExpr);
            if (dr.Length > 0)
            {
                return dr[0]["channelID"].ToString();
            }
            else
            {
            return "UN";
            }
        }
        public static void ProcessFile(string fp )
        {
            string filePath = "";
            StreamWriter SW;
            string logfile = filePath + "\\Log_" + DateTime.Today.ToLongDateString() + ".log";
            //if (!File.Exists(logfile)) { File.CreateText(logfile); }
            //File.AppendText(logfile);
            string[] filePaths = Directory.GetFiles(fp, "*.txt");
            string channelID, transmissionDate, startTime, endTime, duration, Rating;
            string oneline;
            Int32 rcount = 0;
            Int16 fcount = 0;
            string tcount = filePaths.Length.ToString();
            foreach (string filename in filePaths)
            {
                TextReader tr = new StreamReader(filename);
                rcount = 0;
                //rcount++;                
                //Application.DoEvents();
                fcount++;
                ///string f = filename.Substring(1, filename.Length - 5) + ".txt";
                Console.WriteLine("Process File = \t" + filename);
                int top = Console.CursorTop;
                dbcon.Open();
                try
                {
                    while ((oneline = tr.ReadLine()) != null)
                    {
                        string[] line = oneline.Split(new char[] { '\t' });
                        channelID = getChannelName(line[1]);
                        if (!channelID.Equals("UN"))
                        {
                            Console.SetCursorPosition(0, top);
                            Console.Write("\t\tRecords: {0}     ", rcount);
                            transmissionDate = line[0];
                            transmissionDate = "20" + transmissionDate.Substring(6, 2) + "-" + transmissionDate.Substring(3, 2) + "-" + transmissionDate.Substring(0, 2);
                            startTime = line[2];
                            duration = line[3];
                            endTime = Convert.ToDateTime(startTime).AddSeconds(Convert.ToDouble(duration)).ToString("HH:mm:ss");  //line[3];
                            Rating = line[4];
                            string query = "insert into logs.trRating(channelID, transmissionDate, startTime, endTime, duration, Rating) values (" + channelID + ",'" + transmissionDate + "','" + startTime + "','" + endTime + "'," + duration + "," + Rating + ")";
                            insertintoDB(query);

                        }
                        else
                        { }
                        rcount++;
                    }
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    tr.Close();
                    dbcon.Close();
                }
                //Directory.Move(filename, @"D:\Hassan\Backup" + "\\" + Path.GetFileName(filename));
            }

        }
        public static void insertintoDB(string query)
        {
            try
            {
                MySqlCommand msc = new MySqlCommand(query, dbcon);
                msc.ExecuteNonQuery();
                
            }
            catch (Exception e)
            {
                if (e.ToString() == "Duplicate entry '29-2014-02-02-00:01:30' for key 'PRIMARY' ")
                { }
                else
                {
                   // Console.WriteLine(e);
                }
            }
        }
        public static void ftpmove(string fn)
        {           
            try
            {
                string localPath = DMPath + DPath + "\\" + db + "\\";
                string fileName = fn;

                FtpWebRequest requestFTPUploader = (FtpWebRequest)WebRequest.Create("ftp://" + FTPServer + folderName + FTPbackup + "/" + fn);
                requestFTPUploader.Credentials = new NetworkCredential(userName, password);
                requestFTPUploader.Method = WebRequestMethods.Ftp.UploadFile;

                FileInfo fileInfo = new FileInfo(localPath + fileName);
                FileStream fileStream = fileInfo.OpenRead();

                int bufferLength = 1048;
                byte[] buffer = new byte[bufferLength];

                Stream uploadStream = requestFTPUploader.GetRequestStream();
                int contentLength = fileStream.Read(buffer, 0, bufferLength);
               
                while (contentLength != 0)
                {
                    uploadStream.Write(buffer, 0, contentLength);
                    contentLength = fileStream.Read(buffer, 0, bufferLength);
                }
                uploadStream.Close();
                fileStream.Close();
                requestFTPUploader = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            int top = Console.CursorTop;
            Console.SetCursorPosition(0, top);
            Console.WriteLine("File {0} move to FTP backup",fn);
           
            try
            {
                FtpWebRequest renameRequest = (FtpWebRequest)WebRequest.Create("ftp://" + FTPServer + folderName + "/" + fn);
                renameRequest.Credentials = new NetworkCredential(userName, password);
                renameRequest.Method = WebRequestMethods.Ftp.DeleteFile;
                renameRequest.RenameTo = "ftp://" + FTPServer + folderName + "/" + fn;// Test.jpg
                FtpWebResponse renameResponse = (FtpWebResponse)renameRequest.GetResponse();
                renameRequest.GetResponse().Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        public static void mail(string mfile)
        {
            MailMessage message = new MailMessage();
            SmtpClient smtps = new SmtpClient(smtp);
            message.To.Add(to);
            message.Subject = subject;
            message.From = new MailAddress(from); //See the note afterwards...
            message.Body = "Following Ratings are received and uploaded in Media monitors Database\n\n\n" + mfile + "\n\nRegards,\nMedia monitors Team\n\n" + "Note: This is System generated email";


            //  smtp.EnableSsl = true;
            // smtp.Port = 587;
            smtps.DeliveryMethod = SmtpDeliveryMethod.Network;
            smtps.Credentials = new System.Net.NetworkCredential(email, pwd);
            smtps.Send(message);
            Console.WriteLine("Message sent successfully");
        }
    
    }
}