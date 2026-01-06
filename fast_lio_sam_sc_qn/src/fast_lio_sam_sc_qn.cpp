#include "fast_lio_sam_sc_qn.h"

FastLioSamScQn::FastLioSamScQn(const ros::NodeHandle &n_private):
    nh_(n_private),lc_config_(), gc_(lc_config_.gicp_config_)
{
    ////// ROS params
    double loop_update_hz, vis_hz;
    LoopClosureConfig lc_config;
    auto &gc = lc_config.gicp_config_;
    auto &qc = lc_config.quatro_config_;
    /* basic */
    nh_.param<std::string>("/basic/map_frame", map_frame_, "map");
    nh_.param<double>("/basic/loop_update_hz", loop_update_hz, 1.0);
    nh_.param<double>("/basic/vis_hz", vis_hz, 0.5);
    nh_.param<double>("/save_voxel_resolution", voxel_res_, 0.3);
    nh_.param<double>("/quatro_nano_gicp_voxel_resolution", lc_config.voxel_res_, 0.3);
    /* keyframe */
    nh_.param<double>("/keyframe/keyframe_threshold", keyframe_thr_, 1.0);
    nh_.param<int>("/keyframe/nusubmap_keyframes", lc_config.num_submap_keyframes_, 5);
    nh_.param<bool>("/keyframe/enable_submap_matching", lc_config.enable_submap_matching_, false);
    nh_.param<std::string>("/mapupdate/saved_map", saved_map_path_, "");
    nh_.param<bool>("/mapupdate/enable", mapupdate_enable_, false);  // New: Enable map alignment
    nh_.param<double>("/mapupdate/voxel_res", map_voxel_res_, 0.25);  // New: Voxel res for reference map


    reference_map_     = boost::make_shared<pcl::PointCloud<PointType>>();
    reference_map_raw_ = boost::make_shared<pcl::PointCloud<PointType>>();

    if (pcl::io::loadPCDFile<PointType>(saved_map_path_, *reference_map_) == -1)
    {
        ROS_ERROR("Failed to load reference map: %s", saved_map_path_.c_str());
    }
    else if (!reference_map_->empty())
    {
        /* Keep a RAW (non-downsampled) copy for saving */
        *reference_map_raw_ = *reference_map_;

        if (mapupdate_enable_)
        {
            /* Voxelize ONLY the runtime reference map (for ICP efficiency) */
            reference_map_ = voxelizePcd(reference_map_, map_voxel_res_);

            ROS_INFO("Reference map loaded: raw=%zu, voxelized=%zu (voxel=%.2f)",
                    reference_map_raw_->size(),
                    reference_map_->size(),
                    map_voxel_res_);
        }
        else
        {
            ROS_INFO("Reference map loaded (no voxelization), points=%zu",
                    reference_map_raw_->size());
        }
    }

    /* ScanContext */
    nh_.param<double>("/scancontext_max_correspondence_distance",
                      lc_config.scancontext_max_correspondence_distance_,
                      35.0);


    /* nano (GICP config) */
    nh_.param<int>("/nano_gicp/thread_number", gc.nano_thread_number_, 0);
    nh_.param<double>("/nano_gicp/icp_score_threshold", gc.icp_score_thr_, 0.1);
    nh_.param<int>("/nano_gicp/correspondences_number", gc.nano_correspondences_number_, 15);
    nh_.param<double>("/nano_gicp/max_correspondence_distance", gc.max_corr_dist_, 0.01);
    nh_.param<int>("/nano_gicp/max_iter", gc.nano_max_iter_, 32);
    nh_.param<double>("/nano_gicp/transformation_epsilon", gc.transformation_epsilon_, 0.01);
    nh_.param<double>("/nano_gicp/euclidean_fitness_epsilon", gc.euclidean_fitness_epsilon_, 0.01);
    nh_.param<int>("/nano_gicp/ransac/max_iter", gc.nano_ransac_max_iter_, 5);
    nh_.param<double>("/nano_gicp/ransac/outlier_rejection_threshold", gc.ransac_outlier_rejection_threshold_, 1.0);
    /* quatro (Quatro config) */
    nh_.param<bool>("/quatro/enable", lc_config.enable_quatro_, false);
    nh_.param<bool>("/quatro/optimize_matching", qc.use_optimized_matching_, true);
    nh_.param<double>("/quatro/distance_threshold", qc.quatro_distance_threshold_, 30.0);
    nh_.param<int>("/quatro/max_nucorrespondences", qc.quatro_max_num_corres_, 200);
    nh_.param<double>("/quatro/fpfh_normal_radius", qc.fpfh_normal_radius_, 0.3);
    nh_.param<double>("/quatro/fpfh_radius", qc.fpfh_radius_, 0.5);
    nh_.param<bool>("/quatro/estimating_scale", qc.estimat_scale_, false);
    nh_.param<double>("/quatro/noise_bound", qc.noise_bound_, 0.3);
    nh_.param<double>("/quatro/rotation/gnc_factor", qc.rot_gnc_factor_, 1.4);
    nh_.param<double>("/quatro/rotation/rot_cost_diff_threshold", qc.rot_cost_diff_thr_, 0.0001);
    nh_.param<int>("/quatro/rotation/numax_iter", qc.quatro_max_iter_, 50);
    /* results */
    nh_.param<bool>("/result/save_map_bag", save_map_bag_, false);
    nh_.param<bool>("/result/save_map_pcd", save_map_pcd_, false);
    nh_.param<bool>("/result/save_in_kitti_format", save_in_kitti_format_, false);
    nh_.param<std::string>("/result/seq_name", seq_name_, "");
    loop_closure_.reset(new LoopClosure(lc_config));
    /* Initialization of GTSAM */
    gtsam::ISAM2Params isam_params_;
    isam_params_.relinearizeThreshold = 0.01;
    isam_params_.relinearizeSkip = 1;
    isam_handler_ = std::make_shared<gtsam::ISAM2>(isam_params_);
    /* ROS things */
    odom_path_.header.frame_id = map_frame_;
    corrected_path_.header.frame_id = map_frame_;
    package_path_ = ros::package::getPath("fast_lio_sam_sc_qn");
    /* publishers */
    odom_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/ori_odom", 10, true);
    path_pub_ = nh_.advertise<nav_msgs::Path>("/ori_path", 10, true);
    corrected_odom_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/corrected_odom", 10, true);
    corrected_path_pub_ = nh_.advertise<nav_msgs::Path>("/corrected_path", 10, true);
    corrected_pcd_map_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/corrected_map", 10, true);
    corrected_current_pcd_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/corrected_current_pcd", 10, true);
    loop_detection_pub_ = nh_.advertise<visualization_msgs::Marker>("/loop_detection", 10, true);
    realtime_pose_pub_ = nh_.advertise<geometry_msgs::PoseStamped>("/pose_stamped", 10);
    debug_src_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/src", 10, true);
    debug_dst_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/dst", 10, true);
    debug_coarse_aligned_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/coarse_aligned_quatro", 10, true);
    debug_fine_aligned_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/fine_aligned_nano_gicp", 10, true);
    /* subscribers */
    sub_odom_ = std::make_shared<message_filters::Subscriber<nav_msgs::Odometry>>(nh_, "/Odometry", 10);
    sub_pcd_ = std::make_shared<message_filters::Subscriber<sensor_msgs::PointCloud2>>(nh_, "/cloud_registered", 10);
    sub_odom_pcd_sync_ = std::make_shared<message_filters::Synchronizer<odom_pcd_sync_pol>>(odom_pcd_sync_pol(10), *sub_odom_, *sub_pcd_);
    sub_odom_pcd_sync_->registerCallback(boost::bind(&FastLioSamScQn::odomPcdCallback, this, _1, _2));
    sub_save_flag_ = nh_.subscribe("/save_dir", 1, &FastLioSamScQn::saveFlagCallback, this);
    /* Timers */
    loop_timer_ = nh_.createTimer(ros::Duration(1 / loop_update_hz), &FastLioSamScQn::loopTimerFunc, this);
    vis_timer_ = nh_.createTimer(ros::Duration(1 / vis_hz), &FastLioSamScQn::visTimerFunc, this);
    ROS_INFO("Main class, starting node...");
}

void FastLioSamScQn::odomPcdCallback(const nav_msgs::OdometryConstPtr &odom_msg,
                                     const sensor_msgs::PointCloud2ConstPtr &pcd_msg)
{
    // --- 1. Store previous odom ---
    Eigen::Matrix4d last_odom_tf = current_frame_.pose_eig_;

    // --- 2. Create current frame from odom + LiDAR ---
    current_frame_ = PosePcd(*odom_msg, *pcd_msg, current_keyframe_idx_);

    auto t1 = high_resolution_clock::now();

    // --- 3. Realtime pose correction ---
    {
        std::lock_guard<std::mutex> lock(realtime_pose_mutex_);
        odom_delta_ = odom_delta_ * last_odom_tf.inverse() * current_frame_.pose_eig_;
        current_frame_.pose_corrected_eig_ = last_corrected_pose_ * odom_delta_;
        realtime_pose_pub_.publish(poseEigToPoseStamped(current_frame_.pose_corrected_eig_, map_frame_));
        broadcaster_.sendTransform(tf::StampedTransform(
            poseEigToROSTf(current_frame_.pose_corrected_eig_),
            ros::Time::now(),
            map_frame_,
            "robot"));
    }

    // --- 4. Publish transformed current scan ---
    corrected_current_pcd_pub_.publish(
        pclToPclRos(transformPcd(current_frame_.pcd_, current_frame_.pose_corrected_eig_), map_frame_));

    // --- 5. Initialization ---
    if (!is_initialized_)
    {
        keyframes_.push_back(current_frame_);
        updateOdomsAndPaths(current_frame_);

        auto variance_vector = (gtsam::Vector(6) << 1e-4, 1e-4, 1e-4, 1e-2, 1e-2, 1e-2).finished();
        gtsam::noiseModel::Diagonal::shared_ptr prior_noise = gtsam::noiseModel::Diagonal::Variances(variance_vector);
        gtsam_graph_.add(gtsam::PriorFactor<gtsam::Pose3>(0, poseEigToGtsamPose(current_frame_.pose_eig_), prior_noise));
        init_esti_.insert(current_keyframe_idx_, poseEigToGtsamPose(current_frame_.pose_eig_));
        current_keyframe_idx_++;

        loop_closure_->updateScancontext(current_frame_.pcd_);
        is_initialized_ = true;
        return;
    }

    // --- 6. Keyframe check ---
    auto t2 = high_resolution_clock::now();
    if (!checkIfKeyframe(current_frame_, keyframes_.back()))
        return;

    // --- 7. ICP alignment with reference map ---
    if (mapupdate_enable_ && reference_map_ && !reference_map_->empty())
    {
        pcl::PointCloud<PointType>::Ptr aligned(new pcl::PointCloud<PointType>());
        pcl::PointCloud<PointType>::ConstPtr source_cloud(new pcl::PointCloud<PointType>(current_frame_.pcd_));

        nano_gicp_.setInputSource(source_cloud);
        nano_gicp_.setInputTarget(reference_map_);

        // --- Use odom-based pose as initial guess ---
        nano_gicp_.align(*aligned, poseEigToMatrix4f(current_frame_.pose_corrected_eig_).cast<float>());

        double icp_score = nano_gicp_.getFitnessScore();
        if (icp_score < gc_.icp_score_thr_)
        {
            current_frame_.pose_corrected_eig_ = nano_gicp_.getFinalTransformation().cast<double>();
            ROS_INFO("Map alignment successful. ICP score: %.3f (below threshold %.3f)", icp_score, gc_.icp_score_thr_);

            // --- Inject ICP-corrected pose as prior for global propagation ---
            {
                std::lock_guard<std::mutex> lock(graph_mutex_);
                gtsam::Pose3 map_aligned_pose = poseEigToGtsamPose(current_frame_.pose_corrected_eig_);
                auto map_prior_noise = gtsam::noiseModel::Diagonal::Variances(
                    (gtsam::Vector(6) << 1e-4, 1e-4, 1e-4, 1e-2, 1e-2, 1e-2).finished()
                );

                gtsam_graph_.add(gtsam::PriorFactor<gtsam::Pose3>(
                    current_keyframe_idx_,
                    map_aligned_pose,
                    map_prior_noise
                ));
            }
        }
        else
        {
            ROS_WARN("Map alignment rejected. ICP score: %.3f (above threshold %.3f) — using odom-based pose.", icp_score, gc_.icp_score_thr_);
        }
    }
    else
    {
        ROS_WARN_THROTTLE(5.0, "Reference map not loaded, empty, or disabled — skipping map alignment.");
    }

    // --- 8. Save keyframe ---
    {
        std::lock_guard<std::mutex> lock(keyframes_mutex_);
        keyframes_.push_back(current_frame_);
    }

    // --- 9. Update GTSAM graph ---
    auto variance_vector = (gtsam::Vector(6) << 1e-4, 1e-4, 1e-4, 1e-2, 1e-2, 1e-2).finished();
    gtsam::noiseModel::Diagonal::shared_ptr odom_noise = gtsam::noiseModel::Diagonal::Variances(variance_vector);

    gtsam::Pose3 pose_from = poseEigToGtsamPose(keyframes_[current_keyframe_idx_ - 1].pose_corrected_eig_);
    gtsam::Pose3 pose_to   = poseEigToGtsamPose(current_frame_.pose_corrected_eig_);
    {
        std::lock_guard<std::mutex> lock(graph_mutex_);
        gtsam_graph_.add(gtsam::BetweenFactor<gtsam::Pose3>(
            current_keyframe_idx_ - 1,
            current_keyframe_idx_,
            pose_from.between(pose_to),
            odom_noise
        ));
        init_esti_.insert(current_keyframe_idx_, pose_to);
    }
    current_keyframe_idx_++;

    // --- 10. Update ScanContext ---
    loop_closure_->updateScancontext(current_frame_.pcd_);

    // --- 11. Graph optimization & pose propagation ---
    {
        std::lock_guard<std::mutex> lock(graph_mutex_);
        isam_handler_->update(gtsam_graph_, init_esti_);
        gtsam_graph_.resize(0);
        init_esti_.clear();
    }

    {
        std::lock_guard<std::mutex> lock(realtime_pose_mutex_);
        corrected_esti_ = isam_handler_->calculateEstimate();

        // Update all keyframes’ corrected poses
        for (size_t i = 0; i < keyframes_.size(); ++i)
        {
            keyframes_[i].pose_corrected_eig_ = gtsamPoseToPoseEig(corrected_esti_.at<gtsam::Pose3>(i));
        }

        last_corrected_pose_ = keyframes_.back().pose_corrected_eig_;
        odom_delta_ = Eigen::Matrix4d::Identity();
    }

    // --- 12. Update odoms & paths for visualization ---
    {
        std::lock_guard<std::mutex> lock(vis_mutex_);
        updateOdomsAndPaths(current_frame_);
    }

    auto t6 = high_resolution_clock::now();
    ROS_INFO("Keyframe processing times (ms) - total: %.2f",
             duration_cast<microseconds>(t6 - t1).count() / 1e3);
}



void FastLioSamScQn::loopTimerFunc(const ros::TimerEvent &event)
{
    auto &latest_keyframe = keyframes_.back();
    if (!is_initialized_ || keyframes_.empty() || latest_keyframe.processed_)
    {
        return;
    }
    latest_keyframe.processed_ = true;

    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    const int closest_keyframe_idx = loop_closure_->fetchCandidateKeyframeIdx(latest_keyframe, keyframes_);
    if (closest_keyframe_idx < 0)
    {
        return;
    }

    const RegistrationOutput &reg_output = loop_closure_->performLoopClosure(latest_keyframe, keyframes_, closest_keyframe_idx);
    if (reg_output.is_valid_)
    {
        ROS_INFO("\033[1;32mLoop closure accepted. Score: %.3f\033[0m", reg_output.score_);
        const auto &score = reg_output.score_;
        gtsam::Pose3 relative_pose = poseEigToGtsamPose(reg_output.pose_between_eig_);  // Assuming T_latest_to_closest
        auto variance_vector = (gtsam::Vector(6) << score, score, score, score, score, score).finished();
        gtsam::noiseModel::Diagonal::shared_ptr loop_noise = gtsam::noiseModel::Diagonal::Variances(variance_vector);
        {
            std::lock_guard<std::mutex> lock(graph_mutex_);
            // Fixed: Use relative pose directly for BetweenFactor (latest to closest)
            gtsam_graph_.add(gtsam::BetweenFactor<gtsam::Pose3>(latest_keyframe.idx_,
                                                                closest_keyframe_idx,
                                                                relative_pose,
                                                                loop_noise));
        }
        loop_idx_pairs_.push_back({latest_keyframe.idx_, closest_keyframe_idx}); // for vis
        loop_added_flag_vis_ = true;
        loop_added_flag_ = true;
    }
    else
    {
        ROS_WARN("Loop closure rejected. Score: %.3f", reg_output.score_);
    }
    high_resolution_clock::time_point t2 = high_resolution_clock::now();

    debug_src_pub_.publish(pclToPclRos(loop_closure_->getSourceCloud(), map_frame_));
    debug_dst_pub_.publish(pclToPclRos(loop_closure_->getTargetCloud(), map_frame_));
    debug_fine_aligned_pub_.publish(pclToPclRos(loop_closure_->getFinalAlignedCloud(), map_frame_));
    debug_coarse_aligned_pub_.publish(pclToPclRos(loop_closure_->getCoarseAlignedCloud(), map_frame_));

    ROS_INFO("loop: %.1f", duration_cast<microseconds>(t2 - t1).count() / 1e3);
    return;
}

void FastLioSamScQn::visTimerFunc(const ros::TimerEvent &event)
{
    if (!is_initialized_)
    {
        return;
    }

    high_resolution_clock::time_point tv1 = high_resolution_clock::now();
    //// 1. if loop closed, correct vis data
    if (loop_added_flag_vis_)
    // copy and ready
    {
        gtsam::Values corrected_esti_copied;
        pcl::PointCloud<pcl::PointXYZ> corrected_odoms;
        nav_msgs::Path corrected_path;
        {
            std::lock_guard<std::mutex> lock(realtime_pose_mutex_);
            corrected_esti_copied = corrected_esti_;
        }
        // correct pose and path
        for (size_t i = 0; i < corrected_esti_copied.size(); ++i)
        {
            gtsam::Pose3 pose_ = corrected_esti_copied.at<gtsam::Pose3>(i);
            corrected_odoms.points.emplace_back(pose_.translation().x(), pose_.translation().y(), pose_.translation().z());
            corrected_path.poses.push_back(gtsamPoseToPoseStamped(pose_, map_frame_));
        }
        // update vis of loop constraints
        if (!loop_idx_pairs_.empty())
        {
            loop_detection_pub_.publish(getLoopMarkers(corrected_esti_copied));
        }
        // update with corrected data
        {
            std::lock_guard<std::mutex> lock(vis_mutex_);
            corrected_odoms_ = corrected_odoms;
            corrected_path_.poses = corrected_path.poses;
        }
        loop_added_flag_vis_ = false;
    }
    //// 2. publish odoms, paths
    {
        std::lock_guard<std::mutex> lock(vis_mutex_);
        odom_pub_.publish(pclToPclRos(odoms_, map_frame_));
        path_pub_.publish(odom_path_);
        corrected_odom_pub_.publish(pclToPclRos(corrected_odoms_, map_frame_));
        corrected_path_pub_.publish(corrected_path_);
    }

    //// 3. global map
    if (global_map_vis_switch_ && corrected_pcd_map_pub_.getNumSubscribers() > 0) // save time, only once
    {
        pcl::PointCloud<PointType>::Ptr corrected_map(new pcl::PointCloud<PointType>());
        corrected_map->reserve(keyframes_[0].pcd_.size() * keyframes_.size()); // it's an approximated size
        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (size_t i = 0; i < keyframes_.size(); ++i)
            {
                *corrected_map += transformPcd(keyframes_[i].pcd_, keyframes_[i].pose_corrected_eig_);
            }
        }
        const auto &voxelized_map = voxelizePcd(corrected_map, voxel_res_);
        corrected_pcd_map_pub_.publish(pclToPclRos(*voxelized_map, map_frame_));
        global_map_vis_switch_ = false;
    }
    if (!global_map_vis_switch_ && corrected_pcd_map_pub_.getNumSubscribers() == 0)
    {
        global_map_vis_switch_ = true;
    }
    high_resolution_clock::time_point tv2 = high_resolution_clock::now();
    ROS_INFO("vis: %.1fms", duration_cast<microseconds>(tv2 - tv1).count() / 1e3);
    return;
}

void FastLioSamScQn::saveFlagCallback(const std_msgs::String::ConstPtr &msg)
{
    if (keyframes_.empty())
    {
        ROS_WARN("No keyframes available, skipping save.");
        return;
    }

    std::string save_dir = msg->data.empty() ? package_path_ : msg->data;
    std::string seq_directory   = save_dir + "/" + seq_name_;
    std::string scans_directory = seq_directory + "/scans";

    /* ================== SAVE SCANS + POSES ================== */
    if (save_in_kitti_format_)
    {
        if (seq_name_.empty())
        {
            ROS_ERROR("seq_name is empty — refusing to delete/create directories.");
            return;
        }

        if (fs::exists(seq_directory))
            fs::remove_all(seq_directory);

        fs::create_directories(scans_directory);

        std::ofstream kitti_pose_file(seq_directory + "/poses_kitti.txt");
        std::ofstream tum_pose_file(seq_directory + "/poses_tum.txt");
        tum_pose_file << "#timestamp x y z qx qy qz qw\n";

        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (size_t i = 0; i < keyframes_.size(); ++i)
            {
                std::stringstream ss;
                ss << scans_directory << "/"
                   << std::setw(6) << std::setfill('0') << i << ".pcd";

                pcl::io::savePCDFileBinary<PointType>(ss.str(), keyframes_[i].pcd_);

                const auto &pose = keyframes_[i].pose_corrected_eig_;
                kitti_pose_file
                    << pose(0,0) << " " << pose(0,1) << " " << pose(0,2) << " " << pose(0,3) << " "
                    << pose(1,0) << " " << pose(1,1) << " " << pose(1,2) << " " << pose(1,3) << " "
                    << pose(2,0) << " " << pose(2,1) << " " << pose(2,2) << " " << pose(2,3) << "\n";

                auto pose_msg = poseEigToPoseStamped(keyframes_[i].pose_corrected_eig_);
                tum_pose_file << std::fixed << std::setprecision(8)
                              << keyframes_[i].timestamp_ << " "
                              << pose_msg.pose.position.x << " "
                              << pose_msg.pose.position.y << " "
                              << pose_msg.pose.position.z << " "
                              << pose_msg.pose.orientation.x << " "
                              << pose_msg.pose.orientation.y << " "
                              << pose_msg.pose.orientation.z << " "
                              << pose_msg.pose.orientation.w << "\n";
            }
        }

        ROS_INFO("Scans + KITTI/TUM poses saved to %s", seq_directory.c_str());
    }

    /* ================== SAVE ROSBAG ================== */
    if (save_map_bag_)
    {
        rosbag::Bag bag;
        bag.open(package_path_ + "/result.bag", rosbag::bagmode::Write);

        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (const auto &kf : keyframes_)
            {
                ros::Time t;
                t.fromSec(kf.timestamp_);
                bag.write("/keyframe_pcd",  t, pclToPclRos(kf.pcd_, map_frame_));
                bag.write("/keyframe_pose", t, poseEigToPoseStamped(kf.pose_corrected_eig_));
            }
        }

        bag.close();
        ROS_INFO("Rosbag saved successfully.");
    }

    /* ================== SAVE MAP PCD (CORRECTED) ================== */
    if (save_map_pcd_)
    {
        pcl::PointCloud<PointType>::Ptr corrected_map(
            new pcl::PointCloud<PointType>());

        corrected_map->reserve(
            keyframes_[0].pcd_.size() * keyframes_.size());

        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (const auto &kf : keyframes_)
            {
                *corrected_map += transformPcd(
                    kf.pcd_,
                    kf.pose_corrected_eig_);
            }
        }

        /* Voxelize ONLY corrected SLAM map */
        pcl::PointCloud<PointType>::Ptr voxelized_corrected_map =
            voxelizePcd(corrected_map, voxel_res_);

        /* Append reference map WITHOUT voxelization */
        if (reference_map_ && !reference_map_->empty())
        {
            *voxelized_corrected_map += *reference_map_raw_;
            ROS_INFO("Reference map appended (%zu pts)", reference_map_->size());
        }

        std::string map_path =
            seq_directory + "/" + seq_name_ + "_map.pcd";

        if (pcl::io::savePCDFileBinary<PointType>(
                map_path, *voxelized_corrected_map) == 0)
        {
            ROS_INFO("\033[32;1mFinal map saved to %s\033[0m",
                     map_path.c_str());
        }
        else
        {
            ROS_ERROR("Failed to save map PCD!");
        }
    }
}


FastLioSamScQn::~FastLioSamScQn()
{
    // save map
    if (save_map_bag_)
    {
        rosbag::Bag bag;
        bag.open(package_path_ + "/result.bag", rosbag::bagmode::Write);
        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (size_t i = 0; i < keyframes_.size(); ++i)
            {
                ros::Time time;
                time.fromSec(keyframes_[i].timestamp_);
                bag.write("/keyframe_pcd", time, pclToPclRos(keyframes_[i].pcd_, map_frame_));
                bag.write("/keyframe_pose", time, poseEigToPoseStamped(keyframes_[i].pose_corrected_eig_));
            }
        }
        bag.close();
        ROS_INFO("\033[36;1mResult saved in .bag format!!!\033[0m");
    }
    /* ================== SAVE MAP PCD ================== */
    if (save_map_pcd_)
    {
        if (keyframes_.empty())
        {
            ROS_WARN("No keyframes available, skipping map PCD save.");
            return;
        }

        // 1. Build corrected map from keyframes ONLY
        pcl::PointCloud<PointType>::Ptr corrected_map(
            new pcl::PointCloud<PointType>());

        corrected_map->reserve(
            keyframes_[0].pcd_.size() * keyframes_.size());

        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (size_t i = 0; i < keyframes_.size(); ++i)
            {
                *corrected_map += transformPcd(
                    keyframes_[i].pcd_,
                    keyframes_[i].pose_corrected_eig_);
            }
        }

        // 2. Voxelize ONLY the corrected keyframe map
        pcl::PointCloud<PointType>::Ptr voxelized_corrected_map =
            voxelizePcd(corrected_map, voxel_res_);

        ROS_INFO("Voxelized corrected map points: %zu",
                voxelized_corrected_map->size());

        // 3. Append reference map WITHOUT voxelization
        if (reference_map_ && !reference_map_->empty())
        {
            *voxelized_corrected_map += *reference_map_raw_;
            ROS_INFO("Reference map appended (not voxelized), points: %zu",
                    reference_map_->size());
        }

        // 4. Save final map
        std::string save_path = package_path_ + "/result.pcd";
        if (pcl::io::savePCDFileBinary<PointType>(
                save_path, *voxelized_corrected_map) == 0)
        {
            ROS_INFO("\033[32;1mResult saved in .pcd binary format at %s\033[0m",
                    save_path.c_str());
        }
        else
        {
            ROS_ERROR("Failed to save result.pcd!");
        }
    }
}

void FastLioSamScQn::updateOdomsAndPaths(const PosePcd &pose_pcd_in)
{
    odoms_.points.emplace_back(pose_pcd_in.pose_eig_(0, 3),
                               pose_pcd_in.pose_eig_(1, 3),
                               pose_pcd_in.pose_eig_(2, 3));
    corrected_odoms_.points.emplace_back(pose_pcd_in.pose_corrected_eig_(0, 3),
                                         pose_pcd_in.pose_corrected_eig_(1, 3),
                                         pose_pcd_in.pose_corrected_eig_(2, 3));
    odom_path_.poses.emplace_back(poseEigToPoseStamped(pose_pcd_in.pose_eig_, map_frame_));
    corrected_path_.poses.emplace_back(poseEigToPoseStamped(pose_pcd_in.pose_corrected_eig_, map_frame_));
    return;
}

visualization_msgs::Marker FastLioSamScQn::getLoopMarkers(const gtsam::Values &corrected_esti_in)
{
    visualization_msgs::Marker edges;
    edges.type = 5u;
    edges.scale.x = 0.12f;
    edges.header.frame_id = map_frame_;
    edges.pose.orientation.w = 1.0f;
    edges.color.r = 1.0f;
    edges.color.g = 1.0f;
    edges.color.b = 1.0f;
    edges.color.a = 1.0f;
    for (size_t i = 0; i < loop_idx_pairs_.size(); ++i)
    {
        if (loop_idx_pairs_[i].first >= corrected_esti_in.size() ||
            loop_idx_pairs_[i].second >= corrected_esti_in.size())
        {
            continue;
        }
        gtsam::Pose3 pose = corrected_esti_in.at<gtsam::Pose3>(loop_idx_pairs_[i].first);
        gtsam::Pose3 pose2 = corrected_esti_in.at<gtsam::Pose3>(loop_idx_pairs_[i].second);
        geometry_msgs::Point p, p2;
        p.x = pose.translation().x();
        p.y = pose.translation().y();
        p.z = pose.translation().z();
        p2.x = pose2.translation().x();
        p2.y = pose2.translation().y();
        p2.z = pose2.translation().z();
        edges.points.push_back(p);
        edges.points.push_back(p2);
    }
    return edges;
}

bool FastLioSamScQn::checkIfKeyframe(const PosePcd &pose_pcd_in, const PosePcd &latest_pose_pcd)
{
    return keyframe_thr_ < (latest_pose_pcd.pose_corrected_eig_.block<3, 1>(0, 3) - pose_pcd_in.pose_corrected_eig_.block<3, 1>(0, 3)).norm();
}