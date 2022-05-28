import os
os.chdir('/var/scripts/platforms')
from vk import Vky
from te import Telegramy
from fb import Facebooky
from tw import Twittery

if __name__ == '__main__':
    v = Telegramy()
    v.insertData()
    v = Vky()
    v.insertData()
    v= Facebooky()
    v.insertData()
    v=Twittery()
    v.insertData()
