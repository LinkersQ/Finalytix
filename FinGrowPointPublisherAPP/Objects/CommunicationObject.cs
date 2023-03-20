using System;
using System.Collections.Generic;
using System.Text;
using Telegram.Bot.Types;

namespace FinGrowPointPublisherAPP.Objects
{
    public class CommunicationObject
    {
        private string _id;
        private string _external_id;
        private DateTime _create_dt;
        private string _message_content;
        private string _message_media;
        private DateTime _communication_dt;
        private string _communication_status;
        private string _inform_messages;
        private string _communication_id_from_channel;


        public string id { get { return _id; } set { _id = value; } }
        public string external_id { get { return _external_id; } set { _external_id = value; } }
        public DateTime create_dt { get { return _create_dt; } set { _create_dt = value; } }
        public string message_content { get { return _message_content; } set { _message_content = value; } }
        public string message_media { get { return _message_media; } set { _message_media = value; } }
        public DateTime communication_dt { get { return _communication_dt; } set { _communication_dt = value; } }
        public string communication_status { get { return _communication_status; } set { _communication_status = value; } }
        public string inform_messages { get { return _inform_messages; } set { _inform_messages = value; } }
        public string communication_id_from_channel { get { return _communication_id_from_channel; } set { _communication_id_from_channel = value; } }


    }
}
