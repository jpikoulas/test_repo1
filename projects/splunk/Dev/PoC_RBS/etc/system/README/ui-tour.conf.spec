#   Version 6.3.1
#
# This file contains the tours available for Splunk Onboarding
#
# There is a default ui-tour.conf in $SPLUNK_HOME/etc/system/default. 
# To create custom tours, place a ui-tour.conf in
# $SPLUNK_HOME/etc/system/local/. To create custom tours for an app, place
# ui-tour.conf in $SPLUNK_HOME/etc/apps/<app_name>/local/.
#
# To learn more about configuration files (including precedence) see the
# documentation located at
# http://docs.splunk.com/Documentation/Splunk/latest/Admin/Aboutconfigurationfiles
#
# GLOBAL SETTINGS
# Use the [default] stanza to define any global settings.
#   * You can also define global settings outside of any stanza, at the top of
#     the file.
#   * This is not a typical conf file for configurations. It is used to set/create
#   * tours to demonstrate product functionality to users.
#   * If an attribute is defined at both the global level and in a specific
#     stanza, the value in the specific stanza takes precedence.

[<stanza name>]
* Stanza name is the name of the tour

useTour = <string>
nextTour = <string>
intro = <string for the introduction if needed>
type = <image || interactive>
label = <string>
tourPage = <string>
viewed = <boolean>

# For image based tours
imageName<int> = image_name_of_first_image.png
imageCaption<int> = <optional text to accompany the above image>
imgPath = <path to images if outside the main "img" folder>
context = <system || <specific app name>>

# For interactive tours
urlData = <string>
stepText<int> = <string for step n>
stepElement<int> = <selector for step n>
stepPosition<int> = <bottom || right || left || top>
stepClickEvent<int> = <click || mousedown || mouseup> for step n
stepClickElement<int> = <selector post click for step n>
