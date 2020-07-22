import os
from PIL import Image

rootdir = '/home/sivam/GITREPO/aws-glue-libs/python/GoogleImagesDownloader/data'
destdir = '/home/sivam/GITREPO/aws-glue-libs/python/GoogleImagesDownloader/filtered'

'''
for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        # print(os.path.join(subdir, file))
        if file.endswith('.jpg'):
            try:
                img = Image.open(os.path.join(subdir, file))  # open the image file
                img.verify()  # verify that it is, in fact an image
            except (IOError, SyntaxError) as e:
                print('Bad file:', os.path.join(subdir, file))  # print out the names of corrupt files
                # if os.path.exists(os.path.join(subdir, file)):
                #     os.remove(os.path.join(subdir, file))
'''
from shutil import copyfile


for subdir, dirs, files in os.walk(rootdir):
    for dirc in dirs:
        print(dirc)
        os.mkdir(os.path.join(destdir, dirc))
        for subdir1, dirs1, files1 in os.walk(os.path.join(rootdir, dirc)):
            for file in files1:
                print(file)
                if file.endswith('.jpg') and int(os.path.splitext(file)[0]) < 5:
                    copyfile(os.path.join(subdir1,file), os.path.join(os.path.join(destdir, dirc), file))
