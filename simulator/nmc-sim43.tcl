#!/usr/bin/tclsh
variable chan
variable connected
set elpos_commanded 0
set elpos 0
set elrate 0
set xelpos_commanded 0
set xelpos 0
set xelrate 0
set ratetime 0

set temperature 20
set pressure 1011
set windspeed 10
set winddirection 90
set precipitation 0
set humidity 44
set WxHr 0
set WxMin 0
set WxSec 0

set AzimuthPredictedAngle 0
set AzimuthAngle 313.491096
set ElevationPredictedAngle 0
set ElevationAngle 43.204422
set AzimuthTrackingError 0
set ElevationTrackingError 0
set Status 0
set AxisAngleTime 0
set AzimuthManualOffset 0
set ElevationManualOffset 0
set ElevationPositionOffset 0
set CrossElevationPositionOffset 0
set AzimuthAngleWrap 0

proc setatten { value } {
    # 2400 8n1 no fc no sc no other funny business
    catch { exec echo 1:0:cbb:0:0:0:0:0:0:5:1:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0 | stty -F /dev/ttyS0 }

    #for auto atten 1, send the attenuation as a raw byte 0-63 + 64
#    set setting [ format "%2X" [ expr $value + 64 ] ]
    set setting [ expr $value + 64 ]
    #puts "attenuator: $setting"

    #catch { exec  echo -n \$'\\x${setting}' > /dev/ttyS0 }

    set port [ open /dev/ttyS0 w ]

    puts $port [ binary format c $setting ]

    close $port

}

proc change_offset { pos_commanded pos pos_rate } {
  set diff [expr $pos_commanded - $pos]
  set dir 1
  if {$diff < 0} {
    set dir -1
  }
  while {[expr abs($diff) > 1.0]} {
    set pos [expr $dir*$pos_rate + $pos]
    puts "Current pos $pos"
    set diff [expr $pos_commanded - $pos]
    after 1000
  }
}

proc recvd { rchan client clport } {
    global chan done
    global elpos
    global elrate
    global elpos_commanded
    global xelpos
    global xelrate
    global xelpos_commanded
    global ratetime

    global temperature
    global pressure
    global windspeed
    global winddirection
    global precipitation
    global humidity
    global WxHr
    global WxMin
    global WxSec

    global AzimuthPredictedAngle
    global AzimuthAngle
    global ElevationPredictedAngle
    global ElevationAngle
    global AzimuthTrackingError
    global ElevationTrackingError
    global Status
    global AxisAngleTime
    global AzimuthManualOffset
    global ElevationManualOffset
    global ElevationPositionOffset
    global CrossElevationPositionOffset
    global AzimuthAngleWrap

    set times 0

    #finish the vwait
    set connected 1
    puts "Connection $rchan received from $client:$clport"
    set chan $rchan

    while { ! [ eof $chan ] } {
    	set data [ gets $chan ]
    	puts "Received: $data"
    	set params [ split $data " " ]
    	switch [ lindex $params 0 ]  {
          PARAM {
            # get the current value of any global variable(s).
            set params_no_keyword [lrange $params 1 [llength $params]]
            set response [list]
            foreach param $params_no_keyword {
              if {[info exists $param]} {
                lappend response [set $param]
              } else {
                puts "parameter $param doesn't exist"
                lappend response "None"
              }
            }
            set response [join $response ", "]
          }
          GET_WEATHER {
            set response "Completed $temperature $pressure $humidity $windspeed $winddirection $precipitation $WxHr:$WxMin:$WxSec"
          }
    	    GET_OFFSETS {
        		set elposm [ expr $elpos / 1000.0 ]
        		set xelposm [ expr $xelpos / 1000.0 ]
        		set ratelength [ expr  [ clock seconds ] - $ratetime ]
        		set elrateacc [ expr ( $elrate * $ratelength ) / 1000.0 ]
        		set xelrateacc [ expr (  $xelrate * $ratelength ) /100.0 ]
        		switch [ lindex $params 1 ]  {
        		    XELEL {
        			set response "Completed $xelposm $xelrateacc $elposm $elrateacc [clock seconds]"
        		    }
        		    default {
        			set response "Completed [ expr $xelposm + $xelrateacc ] [ expr $elposm + $elrateacc ] [clock seconds]"
        		    }
        		}
    	    }
    	    GET_AZEL {
    		      set response "Completed $AzimuthAngle $ElevationAngle 0 1133945034"
    	    }

    	    TERMINATE {
        		catch { close $chan }
        		set done 1
    	    }
    	    ONSOURCE {
        		if { $times > 2 } {
        		    set times 0
        		    set response "COMPLETED ONSOURCE"
        		} else {
        		    set times [expr $times + 1 ]
        		    set response "COMPLETED SLEWING"
        		}
        	}
        	WAITONSOURCE {
        		sleep 10
        		set response "COMPLETED ONSOURCE"
    	    }
    	    ANTENNA {
        		switch [ lindex $params 1] {
        		   "HI" {
        			      set response "COMPLETED. ACA: ANT-6140-OP BV2.0.0 052505 and AMC: ANT-6146-OP-BV2.0.0 053005"
        		    }
        		    RO {
                  puts "ANTENNA RO: Setting offset rate"
            			switch [ lindex $params 2] {
            			    EL { set elrate [ lindex $params 3 ] }
            			    XEL { set xelrate [ lindex $params 3 ]  }
            			}
            			set ratetime [ clock seconds ]
            			# fake a gaussian with the right beamwidth based on the offset in whatever axis
            			# using the auto-attenuators in the block 0 rack (note: PM1 connected to AA 1)
            			#set offset [lindex $params 3]
            			#puts "[lindex $params 3] $offset "
            			#set amplitude  [ expr exp( -2.7725887 * pow( [ expr $offset / 14.0 ], 2.0 )  ) * 30.0  ]
            			#puts $amplitude
            			#setatten [ expr 30 - int( $amplitude ) ]
        			    set response "COMPLETED"
        		    }
        		    PO {
                  puts "ANTENNA PO: Setting offset"
            			switch [ lindex $params 2] {
            			    EL {
                        set elpos_commanded [ lindex $params 3 ]
                        #change_offset $elpos_commanded $elpos $elrate
                        set elpos [ lindex $params 3 ]
                        set ElevationPositionOffset [ lindex $params 3 ]
                        puts "ANTENNA PO EL: el offset: $ElevationPositionOffset"
                      }
            			    XEL {
                        set xelpos_commanded [ lindex $params 3 ]
                        set xelpos [ lindex $params 3 ]
                        set CrossElevationPositionOffset [ lindex $params 3 ]
                        puts "ANTENNA PO XEL: xel offset: $CrossElevationPositionOffset"
                      }
            			}
        			    set response "COMPLETED"
        		    }
        		    CLR {
            			#		  if { [lindex $params 2] == "PO" } {
            			#set the auto attenuators to a default mid value
            			#setatten 30
            			#		  } else {
            			#      	          }
            			switch [ lindex $params 2] {
            			    RO {
                				switch [ lindex $params 3] {
                				    EL { set elrate 0 }
                				    XEL { set xelrate 0  }
                				}
            			    }
            			    PO {
                				switch [ lindex $params 3] {
                				    EL {  set elpos 0 }
                				    XEL { set xelpos 0  }
                				}
            			   }
            			}
        		    }
        		    default {
                    puts "ANTENNA default: setting default response"
      			        set response "COMPLETED"
        		    }
        		}
    	    }
    	    ATTEN {
        		setatten [ expr int ( [ lindex $params 1 ] ) ]
        		set response "COMPLETED"
    	    }
    	    default {
             puts "default: setting default response"
    		     set response "COMPLETED"
    	    }
    	}
    	catch { puts $chan $response }
    	puts "--> sent: $response"
    	catch { flush $chan }
    }
    puts "Connection $rchan closed."
}

socket -server recvd 6743
vwait done
