import json
import os


def text_to_json_Profiles_table():

    if os.path.isfile("/temp/data.json"):
        print("data.json dataset already exists")
    else:
        profile_data = []
        columns = ['user_id','public','completion_percentage','gender','region',
                   'last_login','registration','AGE','body','I_am_working_in_field','spoken_languages','hobbies',
                   'I_most_enjoy_good_food','pets','body_type','my_eyesight','eye_color','hair_color','hair_type',
                   'completed_level_of_education','favourite_color','relation_to_smoking','relation_to_alcohol','sign_in_zodiac','on_pokec_i_am_looking_for','love_is_for_me',
                   'relation_to_casual_sex','my_partner_should_be','marital_status','children','relation_to_children','I_like_movies','I_like_watching_movie',
                   'I_like_music','I_mostly_like_listening_to_music','the_idea_of_good_evening','I_like_specialties_from_kitchen','fun','I_am_going_to_concerts','my_active_sports',
                   'my_passive_sports','profession','I_like_books','life_style','music','cars','politics',
                   'relationships','art_culture','hobbies_interests''science_technologies','computers_internet','education',
                   'sport','movies''travelling','health','companies_brands','more'
                ]
        with open('/temp/soc-pokec-profiles.txt', 'r', encoding="utf8") as data:
            for line in data:
                line = line.strip('\n')
                ldata = line.split('\t')
                if ldata[0] == "500":
                    break
                temp_profile_data = {}
                for i in range(len(columns)):
                    temp_profile_data[columns[i]] = ldata[i]
                profile_data.append(temp_profile_data)

        with open('/temp/data.json', 'w') as fp:
            json.dump(profile_data, fp, indent=4)
        #from pprint import pprint
        #pprint(profile_data)

        #os.remove("./temp/soc-pokec-profiles.txt")


def text_to_json_Relationship_table():

    if os.path.isfile("/temp/relations.json"):
        print("relations.json dataset already exists")
    else:
        relationship_data = []
        columns = ['_from','_to']

        with open('/temp/soc-pokec-relationships.txt', 'r', encoding="utf8") as data:
            for line in data:
                line = line.strip('\n')
                ldata = line.split('\t')
                if ldata[0] == "83" and ldata[1] == "619":
                    break
                temp_profile_data = {}
                for i in range(len(columns)):
                    temp_profile_data[columns[i]] = ldata[i]
                relationship_data.append(temp_profile_data)

        with open('/temp/relations.json', 'w') as fp:
            json.dump(relationship_data, fp, indent=4)
        #from pprint import pprint
        #pprint(relationship_data)

        #os.remove("./temp/soc-pokec-relationships.txt")


if __name__ == "__main__":

    text_to_json_Profiles_table()

    text_to_json_Relationship_table()